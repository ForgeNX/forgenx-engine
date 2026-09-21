package mesh

import (
	"math"
	"sort"
	"strings"
	"time"
)

// The balancer places System Mesh miners on coins so their combined hashrate is
// split the way the System Mesh target says. Only miners handed to the System
// Mesh take part; a miner the user has pinned or split keeps its own allocation
// and is not counted.
//
// It is deliberately reluctant. Every move costs a switch, so it does nothing
// while each coin is within balanceTolerance of target, moves at most
// maxMovesPerRound miners at a time, and leaves a miner alone for moveCooldown
// after moving it. Better to sit a little off target than shuffle hardware.
const (
	balanceEvery     = 5 * time.Minute
	balanceTolerance = 0.05
	maxMovesPerRound = 3
	moveCooldown     = 30 * time.Minute
)

// SetBalancer installs what the balancer needs from outside the mesh: the
// System Mesh target, which workers belong to it, and each worker's steady
// hashrate in H/s.
func (m *Mesh) SetBalancer(target func() ([]Weight, bool), isAuto func(worker string) bool, hashrate func(worker string) float64) {
	m.placeMu.Lock()
	m.balTarget, m.balIsAuto, m.balHashrate = target, isAuto, hashrate
	m.placeMu.Unlock()
}

// Rebalance asks for a balancing round now rather than at the next tick — after
// the target changes, or a miner joins or leaves the System Mesh.
func (m *Mesh) Rebalance() {
	select {
	case m.balNudge <- struct{}{}:
	default:
	}
}

func (m *Mesh) balanceLoop() {
	t := time.NewTicker(balanceEvery)
	defer t.Stop()
	// Placements live in memory, so after a restart System Mesh miners follow the
	// default order until the first round. Run one once miners have reconnected
	// and the scanner has had time to read them, rather than waiting a full tick.
	first := time.NewTimer(90 * time.Second)
	defer first.Stop()
	for {
		select {
		case <-first.C:
		case <-t.C:
		case <-m.balNudge:
		}
		m.balance()
	}
}

type balMiner struct {
	worker  string
	hash    float64
	coin    string
	movable bool
	alive   map[string]bool // coins this miner's session can reach right now
}

func (m *Mesh) balance() {
	m.placeMu.Lock()
	targetFn, isAuto, hashFn := m.balTarget, m.balIsAuto, m.balHashrate
	m.placeMu.Unlock()
	if targetFn == nil || isAuto == nil || hashFn == nil {
		return
	}
	weights, ok := targetFn()
	if !ok || len(weights) == 0 {
		return
	}

	// Target shares over the coins the mesh actually carries.
	target := map[string]float64{}
	var sum float64
	for _, w := range weights {
		for _, c := range m.opts.Coins {
			if strings.EqualFold(c, w.Coin) && w.Percent > 0 {
				target[c] += w.Percent
				sum += w.Percent
			}
		}
	}
	if sum <= 0 {
		return
	}
	for c := range target {
		target[c] /= sum
	}

	// Included miners that are connected now, and where each currently is.
	m.liveMu.Lock()
	var miners []*balMiner
	for worker, set := range m.live {
		if !isAuto(worker) {
			continue
		}
		for s := range set {
			bm := &balMiner{worker: worker, alive: map[string]bool{}}
			for _, b := range s.bondedBackends() {
				if b.Alive() {
					bm.alive[b.Symbol] = true
				}
			}
			if a := s.activeBackend(); a != nil {
				bm.coin = a.Symbol
			}
			miners = append(miners, bm)
			break
		}
	}
	m.liveMu.Unlock()

	// Hashrates are looked up only after the lock is released: the fallback,
	// MeasuredHashrates, takes liveMu itself, and taking it twice would hang.
	for _, bm := range miners {
		bm.hash = hashFn(bm.worker)
	}

	m.placeMu.Lock()
	// A miner that has left the System Mesh loses its placement.
	for w := range m.placement {
		if !isAuto(w) {
			delete(m.placement, w)
		}
	}
	for _, bm := range miners {
		if c, ok := m.placement[bm.worker]; ok {
			bm.coin = c
		}
		bm.movable = time.Since(m.movedAt[bm.worker]) >= moveCooldown
	}
	m.placeMu.Unlock()
	if len(miners) == 0 {
		return
	}
	sort.Slice(miners, func(i, j int) bool { return miners[i].hash > miners[j].hash })

	actual := func() (map[string]float64, float64) {
		a := map[string]float64{}
		var total float64
		for _, bm := range miners {
			a[bm.coin] += bm.hash
			total += bm.hash
		}
		return a, total
	}
	// Squared distance from target, and the worst single coin's deviation.
	score := func() (float64, float64) {
		a, total := actual()
		if total <= 0 {
			return 0, 0
		}
		var sq, worst float64
		for c := range target {
			d := a[c]/total - target[c]
			sq += d * d
			worst = math.Max(worst, math.Abs(d))
		}
		for c, v := range a {
			if _, inTarget := target[c]; !inTarget && v > 0 {
				d := v / total
				sq += d * d
				worst = math.Max(worst, d)
			}
		}
		return sq, worst
	}

	original := map[string]string{}
	for _, bm := range miners {
		original[bm.worker] = bm.coin
	}

	for moves := 0; moves < maxMovesPerRound; moves++ {
		current, worst := score()
		if worst <= balanceTolerance {
			break
		}
		var best *balMiner
		var bestCoin string
		bestScore := current
		for _, bm := range miners {
			if !bm.movable || bm.hash <= 0 {
				continue
			}
			from := bm.coin
			for c := range target {
				if c == from || !bm.alive[c] {
					continue
				}
				bm.coin = c
				if sc, _ := score(); sc < bestScore-1e-9 {
					best, bestCoin, bestScore = bm, c, sc
				}
				bm.coin = from
			}
		}
		if best == nil {
			break // no single move helps
		}
		best.coin = bestCoin
		best.movable = false
	}

	// Record every placement; move the miners whose coin changed.
	now := time.Now()
	var moved []*balMiner
	m.placeMu.Lock()
	for _, bm := range miners {
		_, placed := m.placement[bm.worker]
		m.placement[bm.worker] = bm.coin
		if bm.coin != original[bm.worker] {
			m.movedAt[bm.worker] = now
			moved = append(moved, bm)
		} else if !placed {
			moved = append(moved, bm) // first placement: pin it where it is
		}
	}
	m.placeMu.Unlock()

	for _, bm := range moved {
		// A single-coin reassignment sets the miner's home and clears any
		// rotation it had, and moves it at its target's next job.
		if _, err := m.ReassignWorker(bm.worker, []Weight{{Coin: bm.coin, Percent: 100}}); err == nil &&
			bm.coin != original[bm.worker] {
			m.logger.Info("[nexus] balancer: %s %s -> %s", bm.worker, original[bm.worker], bm.coin)
		}
	}
}
