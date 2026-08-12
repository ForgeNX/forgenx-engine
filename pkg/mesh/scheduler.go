package mesh

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// Scheduler drives the proportional time-slice rotation. For each mesh miner it
// runs a rotation loop that, on each tick, selects the next coin by weight,
// pulls that coin's current job (paying the coin's configured address), sends it
// to the miner with clean_jobs on a coin switch, and records a MeshJobContext so
// the returned share can be routed to the right coin's validator.
//
// This is the core of Nexus Mesh. The rotation cadence must be <=30s to satisfy
// miner watchdogs (the notify frequency that keeps a miner from declaring the
// pool dead — the exact behavior validated during the parked-connection tests).
type Scheduler struct {
	registry CoinRegistry
	routes   *RouteMap
	logger   *logging.Logger

	// rotateInterval is how long each coin slice lasts before rotating to the
	// next. Must be <=30s for watchdog safety; also bounds switch overhead.
	rotateInterval time.Duration

	// defaultDiff is the starting per-route difficulty until split vardiff tunes.
	defaultDiff float64

	mu     sync.Mutex
	loops  map[string]*minerLoop // keyed by miner worker/session
	jobSeq atomic.Uint64
}

// minerLoop is one miner's rotation goroutine state.
type minerLoop struct {
	ms    *MinerSession
	stop  chan struct{}
	rotor *weightedRotor
}

// NewScheduler creates the scheduler. rotateInterval is clamped to <=30s.
func NewScheduler(registry CoinRegistry, routes *RouteMap, rotateInterval time.Duration, defaultDiff float64) *Scheduler {
	if rotateInterval <= 0 || rotateInterval > 30*time.Second {
		rotateInterval = 20 * time.Second
	}
	if defaultDiff <= 0 {
		defaultDiff = 1024
	}
	return &Scheduler{
		registry:       registry,
		routes:         routes,
		logger:         logging.New(logging.ModuleStratum),
		rotateInterval: rotateInterval,
		defaultDiff:    defaultDiff,
		loops:          make(map[string]*minerLoop),
	}
}

// StartMiner begins rotation for a newly-authorized mesh miner. If the miner has
// no allocation yet, the loop idles (sending nothing) until one is set — but the
// listener/UI is expected to set a default allocation so the miner gets work and
// stays connected.
func (sch *Scheduler) StartMiner(ms *MinerSession) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	key := ms.Session.ID
	if _, exists := sch.loops[key]; exists {
		return
	}
	ml := &minerLoop{
		ms:    ms,
		stop:  make(chan struct{}),
		rotor: newWeightedRotor(),
	}
	sch.loops[key] = ml
	go sch.runLoop(ml)
	sch.logger.Info("[mesh] scheduler started rotation for miner %s", ms.Worker)
}

// StopMiner ends rotation for a disconnected miner.
func (sch *Scheduler) StopMiner(ms *MinerSession) {
	sch.mu.Lock()
	ml := sch.loops[ms.Session.ID]
	delete(sch.loops, ms.Session.ID)
	sch.mu.Unlock()
	if ml != nil {
		close(ml.stop)
		sch.logger.Info("[mesh] scheduler stopped rotation for miner %s", ms.Worker)
	}
}

// SetAllocation updates a miner's coin split. The rotor picks it up on the next
// tick.
func (sch *Scheduler) SetAllocation(ms *MinerSession, alloc *Allocation) {
	ms.alloc = alloc
}

// runLoop is one miner's rotation goroutine.
func (sch *Scheduler) runLoop(ml *minerLoop) {
	t := time.NewTicker(sch.rotateInterval)
	defer t.Stop()
	var lastCoin string
	for {
		select {
		case <-ml.stop:
			return
		case <-t.C:
			coin := sch.pickCoin(ml)
			if coin == "" {
				continue // no allocation / no available coin this tick
			}
			cleanJobs := coin != lastCoin
			if err := sch.sendJob(ml.ms, coin, cleanJobs); err != nil {
				sch.logger.Debug("[mesh] send job to %s for %s failed: %v", ml.ms.Worker, coin, err)
				continue
			}
			lastCoin = coin
		}
	}
}

// pickCoin selects the next coin for this miner by weighted rotation, skipping
// coins that aren't currently running.
func (sch *Scheduler) pickCoin(ml *minerLoop) string {
	alloc := ml.ms.alloc
	if alloc == nil || alloc.Empty() {
		return ""
	}
	weights := alloc.Weights()
	// Filter to running coins.
	avail := make([]CoinWeight, 0, len(weights))
	for _, w := range weights {
		if w.Percent <= 0 {
			continue
		}
		if h := sch.registry.Coin(w.Coin); h != nil && h.Running() {
			avail = append(avail, w)
		}
	}
	if len(avail) == 0 {
		return ""
	}
	return ml.rotor.next(avail)
}

// sendJob pulls the coin's current job, sends it to the miner, and records the
// context for share routing.
func (sch *Scheduler) sendJob(ms *MinerSession, coinSym string, cleanJobs bool) error {
	h := sch.registry.Coin(coinSym)
	if h == nil {
		return fmt.Errorf("coin %s unavailable", coinSym)
	}
	addr := h.PayoutAddress()
	if addr == "" {
		return fmt.Errorf("coin %s has no payout address", coinSym)
	}
	h.RegisterMeshAddress(addr)
	job := h.JobForAddress(addr)
	if job == nil {
		return fmt.Errorf("coin %s has no current job", coinSym)
	}

	// Assign a mesh-scoped job-ID and record context so the returned share can be
	// routed back to this coin with the right session view.
	meshJobID := fmt.Sprintf("m%x", sch.jobSeq.Add(1))
	sourceJobID := job.JobID

	// Rewrite the job's ID to the mesh job-ID so the miner submits against it and
	// we can look it up. (The coin's own validator looks up by SourceJobID, which
	// we carry in the context; the router will restore it.)
	job.JobID = meshJobID
	job.CleanJobs = cleanJobs

	sch.routes.Put(meshJobID, &MeshJobContext{
		Coin:             coinSym,
		SourceJobID:      sourceJobID,
		MiningAddressVal: addr,
		ExtraNonce1Val:   ms.Session.ExtraNonce1(),
		DifficultyVal:    sch.defaultDiff, // TODO: per-route split vardiff
		CreatedAt:        time.Now(),
	})

	ms.Session.SendJob(job)
	return nil
}

var _ = stratum.ShareSession(nil) // ensure stratum import is used
