package mesh

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
)

// Resolver returns the endpoint for a coin symbol, or ok=false if unknown.
type Resolver func(symbol string) (host string, port int, payout string, running bool, ok bool)

// Options configures the Nexus Mesh relay.
type Options struct {
	Port int // miner-facing listen port (e.g. 3350)

	// Coins lists the coin symbols a miner is bonded to, in preference order. The
	// first that resolves and is running becomes the active coin; the rest are
	// connected and authorized but held warm, capturing jobs without forwarding
	// them, ready for rotation to switch to. A single entry reproduces the
	// original single-coin behaviour exactly.
	Coins []string

	Resolve Resolver // resolves coin symbol -> endpoint

	// Assignment returns how a named worker's hashrate should be allocated, and
	// whether the user has assigned it at all. A miner with no assignment mines
	// whatever Coins puts first, so it is productive from the moment it connects
	// rather than parked waiting to be assigned — a parked miner's watchdog would
	// drop it. Looked up at authorize, not at bond, because that is when the worker
	// name is known. Set after construction via SetAssignmentLookup, since the
	// store it reads from is created after the mesh starts.
	Assignment func(worker string) ([]Weight, bool)

	// Default returns the order a miner with no assignment of its own should bond
	// its coins in: the first is where it starts, the rest are its fallbacks in
	// preference order. Without it the mesh uses the order the coins were configured
	// in, which is an installation detail rather than a choice anyone made.
	//
	// Coins the mesh knows about but the list omits keep their configured order
	// behind the ones it names, so a coin added to the config later still works
	// without anyone revisiting this setting.
	Default func() (order []string, ok bool)

	// RotateCycle is how long a full rotation takes for a miner split across coins.
	// Each coin gets its share of this. Read per session rather than cached, so a
	// change applies to miners already connected at their next boundary.
	RotateCycle func() time.Duration
}

// Weight is one coin's share of a miner's time. A single entry at 100 means the
// miner stays on that coin; several mean it rotates between them.
type Weight struct {
	Coin    string
	Percent float64
}

// primaryCoin returns the first configured coin, for logging before any session
// has resolved which coins are actually available.
func (o Options) primaryCoin() string {
	if len(o.Coins) == 0 {
		return ""
	}
	return o.Coins[0]
}

// Mesh is the Nexus Mesh relay: one stable miner endpoint that bonds each miner
// to a coin's real V1 stratum server (pass-through), so the coin app does its
// own vardiff, block construction, and worker tracking.
type Mesh struct {
	opts     Options
	logger   *logging.Logger
	listener net.Listener
	sessSeq  atomic.Uint64

	// live tracks sessions by worker-name suffix so an assignment made from the UI
	// takes effect on the miner that is connected now, rather than only when it
	// next reconnects. A worker can briefly have more than one session during a
	// reconnect, so each name maps to a set.
	liveMu sync.Mutex
	live   map[string]map[*Session]struct{}

	// placement is where the balancer has put each miner in the System Mesh, by
	// worker. A miner handed to the System Mesh but not yet placed follows the
	// default order until the balancer decides.
	placeMu   sync.Mutex
	placement map[string]string
	movedAt   map[string]time.Time

	balTarget   func() ([]Weight, bool)
	balIsAuto   func(worker string) bool
	balHashrate func(worker string) float64
	balNudge    chan struct{}
	balNetDiff  func(symbol string) float64

	// keepaliveFor reports the active coin's ping settings, for the keepalive.
	keepaliveFor func(symbol string) (bool, time.Duration)

	// Session totals for the Nexus overview, since the engine started.
	startedAt    time.Time
	statAccepted atomic.Uint64
	statRejected atomic.Uint64
	statStale    atomic.Uint64
	statSwitches atomic.Uint64
	statLost     atomic.Uint64 // leftover work from before a reconnect, answered by the relay

	// Per-worker share tallies this session: accepted, rejected, stale.
	tallyMu sync.Mutex
	tallies map[string]*[4]uint64
	// The same outcomes counted per coin, for the Nexus overview's per-node line.
	coinTallies map[string]*[4]uint64
}

func New(opts Options) *Mesh {
	return &Mesh{
		opts:      opts,
		logger:    logging.New(logging.ModuleNexus),
		live:      make(map[string]map[*Session]struct{}),
		placement: make(map[string]string),
		movedAt:   make(map[string]time.Time),
		balNudge:  make(chan struct{}, 1),
		startedAt: time.Now(),
	}
}

func (m *Mesh) Start() error {
	addr := fmt.Sprintf(":%d", m.opts.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nexus listen %s: %w", addr, err)
	}
	m.listener = ln
	m.logger.Info("[nexus] Mesh listening on %s (coins=%v)", addr, m.opts.Coins)
	go m.acceptLoop()
	go m.balanceLoop()
	return nil
}

func (m *Mesh) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			m.logger.Debug("[nexus] accept end: %v", err)
			return
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(true)
			tcp.SetKeepAlive(true)
			tcp.SetKeepAlivePeriod(30 * time.Second)
		}
		go m.handleMiner(conn)
	}
}

func (m *Mesh) handleMiner(conn net.Conn) {
	id := "n" + strconv.FormatUint(m.sessSeq.Add(1), 16)
	s := NewSession(id, conn, m.logger)

	// Bond a backend per configured coin. The first that connects becomes active;
	// the rest are held warm (connected and authorized, capturing jobs but not
	// forwarding them) so rotation can switch to them without a handshake. A coin
	// that is missing, stopped, or unconfigured is skipped rather than failing the
	// whole session — one unavailable coin must not cost the miner its bond.
	// The user's default goes first; the rest follow in configured order as
	// failover. Only the head of the list changes, so a coin dying still falls
	// through to the next one regardless of what the default is.
	coins := m.opts.Coins
	if m.opts.Default != nil {
		if pref, ok := m.opts.Default(); ok && len(pref) > 0 {
			ordered := make([]string, 0, len(coins))
			taken := make(map[string]bool, len(coins))
			for _, want := range pref {
				for _, c := range coins {
					if !taken[c] && strings.EqualFold(c, want) {
						ordered = append(ordered, c)
						taken[c] = true
					}
				}
			}
			for _, c := range coins {
				if !taken[c] {
					ordered = append(ordered, c)
				}
			}
			coins = ordered
		}
	}

	var backends []*Backend
	for _, symbol := range coins {
		host, port, payout, running, ok := m.opts.Resolve(symbol)
		// A coin the resolver cannot describe yet is still bonded, dead. "Not found"
		// here usually means its runner has not started — the node is syncing, or the
		// app was restarted — rather than that the coin does not exist, and in that
		// case skipping left the miner on a fallback with nothing watching for the
		// coin's return. The reconnect loop re-resolves on every attempt, so it picks
		// up the address and payout once the runner appears.
		//
		// A coin genuinely absent from the engine never resolves, so it retries
		// quietly forever. That is the cost of not being able to tell the two apart.
		if !ok {
			m.logger.Info("[nexus] %s: coin %s not resolvable yet; bonding dead and watching", id, symbol)
			host, port, payout, running = "", 0, "", false
		}
		// A coin that is configured but not currently serving is bonded dead rather
		// than skipped: its reconnect loop re-resolves and dials until it comes back,
		// at which point failback can move the miner onto it. Skipping instead left a
		// miner that connected during an outage permanently on its fallback, since a
		// coin absent at bond time was never watched for.
		if !running {
			m.logger.Info("[nexus] %s: coin %s not serving yet; bonding dead and watching", id, symbol)
		}
		// Only a coin that resolved but has no payout is a misconfiguration worth
		// skipping; an unresolvable one has no payout yet by definition and gets it
		// from the resolver when its runner starts.
		if ok && payout == "" {
			m.logger.Warn("[nexus] %s: coin %s has no payout configured; skipping", id, symbol)
			continue
		}
		backendAddr := fmt.Sprintf("%s:%d", host, port)
		b := NewBackend(symbol, backendAddr, payout, "", m.logger)
		sym := symbol
		b.SetResolver(func() (string, string, bool) {
			h, p, pay, run, ok := m.opts.Resolve(sym)
			if !ok {
				return "", "", false
			}
			return fmt.Sprintf("%s:%d", h, p), pay, run
		})
		if !running || !ok {
			b.markDead()
		} else if err := b.Connect(); err != nil {
			// Same treatment as a coin that is not serving: the endpoint is known, the
			// dial failed, so let the reconnect loop keep trying rather than dropping
			// the coin for the life of the session.
			m.logger.Info("[nexus] %s: backend %s connect failed (%v); bonding dead and watching", id, symbol, err)
			b.markDead()
		}
		backends = append(backends, b)
	}

	if len(backends) == 0 {
		m.logger.Warn("[nexus] %s: no coin available to bond; closing", id)
		s.Close()
		return
	}

	symbols := make([]string, 0, len(backends))
	for _, b := range backends {
		sym := b.Symbol
		if !b.Alive() {
			sym += "(down)"
		}
		symbols = append(symbols, sym)
	}
	m.logger.Info("[nexus] %s: miner bonded to %v", id, symbols)

	m.runMiner(s, backends)
}

// SetRotateCycleLookup installs the lookup for the rotation cycle length.
func (m *Mesh) SetRotateCycleLookup(f func() time.Duration) { m.opts.RotateCycle = f }

// SetDefaultLookup installs the lookup for the user's chosen default coin. Set
// after construction for the same reason as the assignment lookup: the store it
// reads from is created after the mesh starts.
func (m *Mesh) SetDefaultLookup(f func() ([]string, bool)) { m.opts.Default = f }

// SetAssignmentLookup installs the per-worker allocation lookup. Safe to call
// after Start: assignments are only read when a miner authorizes, so a miner that
// connects beforehand simply uses the default until it next reconnects.
func (m *Mesh) SetAssignmentLookup(f func(worker string) ([]Weight, bool)) {
	m.opts.Assignment = f
}

// registerLive records a session under its worker name so a reassignment can find
// it while it is connected.
func (m *Mesh) registerLive(worker string, s *Session) {
	if worker == "" {
		return
	}
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	if m.live[worker] == nil {
		m.live[worker] = make(map[*Session]struct{})
	}
	m.live[worker][s] = struct{}{}
}

// unregisterLive drops a session from the registry when it closes.
func (m *Mesh) unregisterLive(worker string, s *Session) {
	if worker == "" {
		return
	}
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	if set := m.live[worker]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(m.live, worker)
		}
	}
}

// Coins returns the coins the mesh is configured to bond, in priority order. The
// UI needs this to offer somewhere to assign a miner to: a coin absent from here
// can never be assigned, however healthy it is.
func (m *Mesh) Coins() []string {
	out := make([]string, len(m.opts.Coins))
	copy(out, m.opts.Coins)
	return out
}

// Enabled reports whether the mesh is listening at all, so the UI can say so
// rather than showing an empty panel.
func (m *Mesh) Enabled() bool { return m != nil && m.listener != nil }

// Port is the miner-facing listen port, shown to the user as where to point a rig.
func (m *Mesh) Port() int { return m.opts.Port }

// MeasuredHashrates reports, per worker, the hashrate the relay has measured from
// the work the miner submitted (H/s) and how many shares it rests on. It follows
// the miner across coin switches, unlike each coin's own average, which keeps
// reporting a stale figure after the miner has left.
func (m *Mesh) MeasuredHashrates() map[string][2]float64 {
	m.liveMu.Lock()
	sessions := make(map[string][]*Session, len(m.live))
	for worker, set := range m.live {
		for s := range set {
			sessions[worker] = append(sessions[worker], s)
		}
	}
	m.liveMu.Unlock()

	out := map[string][2]float64{}
	for worker, list := range sessions {
		for _, s := range list {
			hps, n := s.MeasuredHashrate()
			if prev, ok := out[worker]; !ok || float64(n) > prev[1] {
				out[worker] = [2]float64{hps, float64(n)}
			}
		}
	}
	return out
}

// Placement returns the coin the balancer has put a System Mesh miner on.
func (m *Mesh) Placement(worker string) (string, bool) {
	m.placeMu.Lock()
	defer m.placeMu.Unlock()
	c, ok := m.placement[worker]
	return c, ok
}

// Placements returns every System Mesh placement, by worker.
func (m *Mesh) Placements() map[string]string {
	m.placeMu.Lock()
	defer m.placeMu.Unlock()
	out := make(map[string]string, len(m.placement))
	for w, c := range m.placement {
		out[w] = c
	}
	return out
}

// PendingSwitches reports, per worker, the coin it is waiting to move to. A
// deferred switch does not take effect until the target sends a job, so without
// this the UI would say a reassignment had applied while the miner was still
// visibly on its old coin.
// NextRotations returns when each rotating miner's next scheduled switch falls.
// A miner on a single node, or one the balancer places, does not rotate and is
// absent from the result.
func (m *Mesh) NextRotations() map[string]time.Time {
	m.liveMu.Lock()
	sessions := make(map[string][]*Session, len(m.live))
	for worker, set := range m.live {
		for s := range set {
			sessions[worker] = append(sessions[worker], s)
		}
	}
	m.liveMu.Unlock()

	out := map[string]time.Time{}
	for worker, list := range sessions {
		for _, s := range list {
			if t := s.NextRotation(); !t.IsZero() && time.Now().Before(t) {
				out[worker] = t
			}
		}
	}
	return out
}

func (m *Mesh) PendingSwitches() map[string]string {
	m.liveMu.Lock()
	sessions := make(map[string][]*Session, len(m.live))
	for worker, set := range m.live {
		for s := range set {
			sessions[worker] = append(sessions[worker], s)
		}
	}
	m.liveMu.Unlock()

	out := map[string]string{}
	for worker, list := range sessions {
		for _, s := range list {
			if target, _ := s.pendingSwitch(); target != nil {
				out[worker] = target.Symbol
			}
		}
	}
	return out
}

// MinerFacts returns each connected worker's own address and client string,
// keyed by worker name. The coin behind the relay sees only the relay's
// connection, so a meshed miner would otherwise display as 127.0.0.1 with no
// hardware — the relay's view rather than the miner's.
func (m *Mesh) MinerFacts() map[string][2]string {
	m.liveMu.Lock()
	sessions := make(map[string][]*Session, len(m.live))
	for worker, set := range m.live {
		for s := range set {
			sessions[worker] = append(sessions[worker], s)
		}
	}
	m.liveMu.Unlock()

	out := make(map[string][2]string, len(sessions))
	for worker, list := range sessions {
		for _, s := range list {
			addr, vendor := s.Facts()
			out[worker] = [2]string{addr, vendor}
		}
	}
	return out
}

// ActiveCoins reports which coin each connected mesh worker is actually mining,
// keyed by worker name. A bonded miner is authorized on every one of its coins —
// warm backends have to be, or the coin never sends them the jobs a switch needs
// to replay — so every coin's stratum server sees it as a connected session. Only
// this says which of those sessions is receiving work, which is what a per-coin
// worker list needs in order not to show the same miner as mining everywhere.
func (m *Mesh) ActiveCoins() map[string]string {
	m.liveMu.Lock()
	sessions := make(map[string][]*Session, len(m.live))
	for worker, set := range m.live {
		for s := range set {
			sessions[worker] = append(sessions[worker], s)
		}
	}
	m.liveMu.Unlock()

	out := make(map[string]string, len(sessions))
	for worker, list := range sessions {
		// A worker briefly holds more than one session while reconnecting. The last
		// one to have been given an active backend is the one doing the work.
		for _, s := range list {
			if b := s.activeBackend(); b != nil {
				out[worker] = b.Symbol
			}
		}
	}
	return out
}

// ReassignWorker moves a connected miner to a coin immediately, rather than
// waiting for it to reconnect and re-read its stored assignment. The caller is
// expected to have persisted the assignment first: this only moves what is
// already running, and reports whether it found anything to move so the UI can
// say "applied now" rather than "will apply when the miner reconnects".
//
// A worker can hold more than one session briefly during a reconnect, so every
// session under that name is moved.
func (m *Mesh) ReassignWorker(worker string, weights []Weight) (moved int, err error) {
	if len(weights) == 0 {
		return 0, nil
	}
	// Where to put the miner now: the coin carrying the largest share. For a
	// pinned miner that is its only coin; for a rotating one it is where the
	// cycle starts, and the rotation loop takes over from there.
	symbol := weights[0].Coin
	best := weights[0].Percent
	for _, w := range weights[1:] {
		if w.Percent > best {
			best, symbol = w.Percent, w.Coin
		}
	}
	rotating := len(weights) > 1

	m.liveMu.Lock()
	sessions := make([]*Session, 0, 2)
	for s := range m.live[worker] {
		sessions = append(sessions, s)
	}
	m.liveMu.Unlock()

	if len(sessions) == 0 {
		return 0, nil // not connected; the stored assignment applies on its next connect
	}

	for _, s := range sessions {
		var target *Backend
		for _, b := range s.bondedBackends() {
			if strings.EqualFold(b.Symbol, symbol) {
				target = b
				break
			}
		}
		if target == nil {
			return moved, fmt.Errorf("worker %s is not bonded to %s", worker, symbol)
		}
		// Rotation and home are mutually exclusive: a rotating miner's schedule
		// decides where it belongs, so it must have no home for failback to defer
		// to, and a pinned miner must have no rotation weights left over or the
		// rotation loop would keep moving it after it was pinned.
		if rotating {
			s.setRotation(weights)
			s.setHome(nil)
		} else {
			s.setRotation(nil)
			s.setHome(target)
		}
		if target == s.activeBackend() {
			moved++
			continue
		}
		if !target.Alive() {
			// Home is set, so the failback loop brings it across once the coin is
			// serving again. Not an error — just not immediate.
			continue
		}
		// Deferred for the same reason rotation is: the miner keeps earning on its
		// current coin until the target sends a job, then abandons that work on a
		// real boundary rather than mid-job with a new difficulty already applied.
		m.switchDeferred(s, target)
		moved++
	}
	return moved, nil
}

func (m *Mesh) Stop() {
	if m.listener != nil {
		m.listener.Close()
	}
}

// SetKeepalive installs the per-coin ping settings the keepalive follows.
func (m *Mesh) SetKeepalive(f func(symbol string) (bool, time.Duration)) {
	m.placeMu.Lock()
	m.keepaliveFor = f
	m.placeMu.Unlock()
}

// Overview is the relay's own tally since the engine started.
type Overview struct {
	Since                     time.Time
	Accepted, Rejected, Stale uint64
	Switches                  uint64
	Lost                      uint64
}

// Overview returns the relay's share and switch totals for this session.
func (m *Mesh) Overview() Overview {
	return Overview{
		Since:    m.startedAt,
		Accepted: m.statAccepted.Load(),
		Rejected: m.statRejected.Load(),
		Stale:    m.statStale.Load(),
		Switches: m.statSwitches.Load(),
		Lost:     m.statLost.Load(),
	}
}

// noteSubmitResponse recognises a coin's reply to a share the relay forwarded,
// counts it, and reports whether it was one - in which case the caller passes it
// to the miner, whichever coin sent it. A stale reply is one naming a job the
// coin no longer has; any other refusal counts as rejected.
func (m *Mesh) noteSubmitResponse(s *Session, coin string, line []byte) bool {
	var r struct {
		ID     json.RawMessage `json:"id"`
		Result interface{}     `json:"result"`
		Error  interface{}     `json:"error"`
	}
	if json.Unmarshal(line, &r) != nil || !s.takePendingSubmit(string(r.ID)) {
		return false
	}
	if ok, _ := r.Result.(bool); ok && r.Error == nil {
		m.statAccepted.Add(1)
		m.tallyOn(s.workerName(), coin, 0)
		return true
	}
	stale := false
	if e, isList := r.Error.([]interface{}); isList && len(e) > 0 {
		if code, _ := e[0].(float64); code == 21 {
			stale = true
		}
		if len(e) > 1 {
			if msg, _ := e[1].(string); strings.Contains(strings.ToLower(msg), "stale") || strings.Contains(strings.ToLower(msg), "job not found") {
				stale = true
			}
		}
	}
	if stale {
		m.statStale.Add(1)
		m.tallyOn(s.workerName(), coin, 2)
	} else {
		m.statRejected.Add(1)
		m.tallyOn(s.workerName(), coin, 1)
	}
	return true
}

// tally records one share outcome for a worker: 0 accepted, 1 rejected, 2 stale, 3 lost to a reconnect.
func (m *Mesh) tally(worker string, outcome int) { m.tallyOn(worker, "", outcome) }

// tallyOn records an outcome for a worker and, when known, the coin it was for.
func (m *Mesh) tallyOn(worker, coin string, outcome int) {
	if worker == "" {
		return
	}
	m.tallyMu.Lock()
	defer m.tallyMu.Unlock()
	if m.tallies == nil {
		m.tallies = make(map[string]*[4]uint64)
	}
	t := m.tallies[worker]
	if t == nil {
		t = &[4]uint64{}
		m.tallies[worker] = t
	}
	t[outcome]++
	if coin == "" {
		return
	}
	if m.coinTallies == nil {
		m.coinTallies = make(map[string]*[4]uint64)
	}
	ct := m.coinTallies[coin]
	if ct == nil {
		ct = &[4]uint64{}
		m.coinTallies[coin] = ct
	}
	ct[outcome]++
}

// WorkerShares returns each worker's accepted, rejected and stale shares this
// session, as counted at the relay - so it includes shares the relay answered
// itself, which no coin ever saw.
func (m *Mesh) WorkerShares() map[string][4]uint64 {
	m.tallyMu.Lock()
	defer m.tallyMu.Unlock()
	out := make(map[string][4]uint64, len(m.tallies))
	for w, t := range m.tallies {
		out[w] = *t
	}
	return out
}

// CoinShares returns each coin's accepted, rejected, stale and lost shares this
// session, as counted at the relay.
func (m *Mesh) CoinShares() map[string][4]uint64 {
	m.tallyMu.Lock()
	defer m.tallyMu.Unlock()
	out := make(map[string][4]uint64, len(m.coinTallies))
	for c, t := range m.coinTallies {
		out[c] = *t
	}
	return out
}
