package mesh

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type rpcMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// runMiner drives one miner session bonded to a single backend. It answers the
// miner's subscribe/authorize locally (adopting the backend's extranonce),
// forwards submits to the coin, and relays the coin's jobs/difficulty/responses
// back to the miner.
// How often to check whether a higher-priority coin has come back. Long enough
// that a flapping coin does not bounce the miner between chains, short enough that
// a coin finishing its sync is picked up promptly.
const failbackInterval = 30 * time.Second

// w0Percent returns a coin's weight from a split, or zero if it is not in it.
func w0Percent(weights []Weight, symbol string) float64 {
	for _, w := range weights {
		if strings.EqualFold(w.Coin, symbol) {
			return w.Percent
		}
	}
	return 0
}

// describeWeights renders a split for logging, e.g. "DGB 70% / BCH 30%".
func describeWeights(w []Weight) string {
	parts := make([]string, 0, len(w))
	for _, x := range w {
		parts = append(parts, fmt.Sprintf("%s %.0f%%", x.Coin, x.Percent))
	}
	return strings.Join(parts, " / ")
}

// workerSuffix strips the payout-address prefix a miner authorizes with, leaving
// the name that identifies the hardware. The same machine authorizes as
// <coin-a-address>.Worker001 on one coin and <coin-b-address>.Worker001 on another, so
// only the suffix is stable enough to key an assignment on.
func workerSuffix(worker string) string {
	if i := strings.LastIndex(worker, "."); i >= 0 {
		return worker[i+1:]
	}
	return worker
}

// firstAlive returns the highest-priority live backend, skipping one. Priority is
// the order coins were configured in.
func firstAlive(backends []*Backend, skip *Backend) *Backend {
	for _, b := range backends {
		if b != skip && b.Alive() {
			return b
		}
	}
	return nil
}

// minerKeepalive is how long a miner may go without hearing anything before the
// mesh sends it something. Between new work a coin says nothing, and a direct
// miner is kept alive by the engine's pings - which the mesh answers itself and
// never forwards. A quiet coin such as BCH can go minutes without new work, and
// cgminer treats a pool that silent as broken and reconnects.
const minerKeepalive = 30 * time.Second

// keepaliveLoop keeps the line to a miner active on the same terms the engine
// gives a directly connected miner: it follows the active coin's server-side
// ping setting, checked on every tick, so a change in the coin app applies at
// once and a miner rotating between coins follows whichever it is on. With ping
// off the mesh stays silent too, as a direct miner's line would be.
//
// It resends the coin's current difficulty rather than a ping. A repeated
// set_difficulty with the same value changes nothing for the miner, where a
// repeated job could make some miners restart their nonce range.
func (m *Mesh) keepaliveLoop(s *Session) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		if s.isClosed() {
			return
		}
		b := s.activeBackend()
		if b == nil {
			continue
		}
		enabled, every := true, minerKeepalive
		m.placeMu.Lock()
		lookup := m.keepaliveFor
		m.placeMu.Unlock()
		if lookup != nil {
			enabled, every = lookup(b.Symbol)
		}
		if !enabled || every <= 0 || s.sinceLastSent() < every {
			continue
		}
		if d := b.CachedDifficulty(); d != nil {
			_ = s.SendRaw(d)
		}
	}
}

// deferredSwitchShare is how much of a coin's own slice a pending move may spend
// waiting for that coin's next job, before giving up and switching mid-job.
//
// A fixed bound was the wrong shape. A coin that only sends a job when it finds a
// block can be quiet for ten minutes, so ninety seconds meant almost every move
// to such a coin timed out and switched mid-job anyway — which is the thing
// deferring exists to avoid. Waiting proportionally lets a long cycle wait a long
// time and keeps a short one responsive.
//
// Missing part of a slice costs little: the percentages are a preference about
// payout mix, not a guarantee, and the loop computes position from elapsed time
// so a late switch does not push later ones out. Losing every share in flight
// does cost something. The bound is only there so a coin that stops producing
// jobs while still looking alive cannot strand a miner indefinitely.
const deferredSwitchShare = 0.5

// rotateLoop moves a miner between coins on the schedule the user set. Rather
// than "switch every N", it works out where the miner should be from elapsed time
// within the cycle and corrects if it is elsewhere — so a miner that failed over
// to another coin during an outage rejoins its proper share when the coin
// returns, instead of the schedule drifting by however long the outage lasted.
//
// Time-slicing splits a miner's expected blocks between coins rather than adding
// to them; it is a way to be paid in more than one coin, not a way to find more.
func (m *Mesh) rotateLoop(s *Session, backends []*Backend, cycle time.Duration) {
	if cycle <= 0 {
		return
	}
	// Check often enough to land near each boundary without spinning.
	tick := cycle / 20
	if tick < 5*time.Second {
		tick = 5 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	start := time.Now()
	for range t.C {
		if s.isClosed() {
			return
		}
		weights := s.rotationWeights()
		if len(weights) < 2 {
			s.setNextRotation(time.Time{}) // not rotating: nothing to count down to
			continue
		}

		total := 0.0
		for _, w := range weights {
			total += w.Percent
		}
		if total <= 0 {
			continue
		}

		// Where in the cycle are we, and whose slice is that?
		elapsed := time.Since(start) % cycle
		var acc time.Duration
		want := weights[0].Coin
		for _, w := range weights {
			acc += time.Duration(float64(cycle) * (w.Percent / total))
			if elapsed < acc {
				want = w.Coin
				break
			}
		}

		// acc is where the current slice ends, so that is the next switch. The
		// loop already works this out each tick; recording it lets the tab say
		// when a rotating miner will move rather than only that it will.
		s.setNextRotation(time.Now().Add(acc - elapsed))

		cur := s.activeBackend()
		m.logger.Debug("[nexus] %s: rotate tick elapsed=%s want=%s cur=%s",
			s.id, elapsed.Round(time.Second), want, symbolOf(cur))
		if cur != nil && strings.EqualFold(cur.Symbol, want) {
			continue
		}
		for _, b := range backends {
			if !strings.EqualFold(b.Symbol, want) {
				continue
			}
			if !b.Alive() {
				// Its turn, but the coin is down. Stay put and earn on something
				// rather than idle on a dead coin to honour a schedule.
				break
			}
			// Already waiting on this coin's next job: let it land, unless it has
			// been quiet long enough that waiting costs more than switching
			// mid-job would.
			if target, waiting := s.pendingSwitch(); target == b {
				// This coin's own slice, halved: how long it may wait for a job
				// before the wait costs more than the mid-job switch would.
				share := w0Percent(weights, b.Symbol) / total
				limit := time.Duration(float64(cycle) * share * deferredSwitchShare)
				if waiting > limit {
					m.logger.Info("[nexus] %s: %s quiet for %s (limit %s); rotating %s -> %s anyway",
						s.id, b.Symbol, waiting.Round(time.Second), limit.Round(time.Second),
						symbolOf(cur), b.Symbol)
					m.switchTo(s, b, nil)
				}
				break
			}
			m.switchDeferred(s, b)
			break
		}
	}
}

// failbackLoop watches for a higher-priority coin becoming available again and
// moves the miner back to it. Runs until the session closes.
func (m *Mesh) failbackLoop(s *Session, backends []*Backend) {
	t := time.NewTicker(failbackInterval)
	defer t.Stop()
	for range t.C {
		if s.isClosed() {
			return
		}
		func() {
			cur := s.activeBackend()
			if cur == nil {
				return
			}
			// A rotating miner has no home — its schedule decides where it should
			// be — so failback leaves it entirely alone. Without this the two
			// fight: rotation moves the miner on, failback drags it back, and
			// neither wins for longer than a ticker interval.
			if len(s.rotationWeights()) > 0 {
				return
			}

			// A miner with an assigned coin belongs there whenever it is up; one
			// without falls back to configured order. Comparing against list
			// position alone would undo an assignment on the next tick.
			if home := s.homeBackend(); home != nil {
				if home != cur && home.Alive() {
					m.logger.Info("[nexus] %s: %s is available again; returning from %s", s.id, home.Symbol, cur.Symbol)
					m.switchActive(s, home)
				}
				return
			}
			for _, b := range backends {
				if b == cur {
					break // nothing ahead of the current coin is alive
				}
				if b.Alive() {
					m.logger.Info("[nexus] %s: %s is available again; failing back from %s", s.id, b.Symbol, cur.Symbol)
					m.switchActive(s, b)
					break
				}
			}
		}()
	}
}

// switchActive moves a bonded miner from its current coin to another without
// dropping the connection where the miner supports it. The miner keeps hashing
// throughout: it is sent the new coin's extranonce, then its cached difficulty and
// latest job, so it starts on the new chain from the next nonce rather than after a
// reconnect. Work already in flight for the previous coin still routes correctly,
// because submits are looked up by job ID in the session registry rather than being
// sent to whichever coin is active now.
//
// A miner that never sent mining.extranonce.subscribe cannot be told its extranonce
// changed; sending it the new coin's jobs would produce shares built on the wrong
// extranonce, which the coin would reject. Those sessions are closed instead so the
// miner reconnects and re-bonds cleanly — a few seconds of lost work, but no stream
// of rejects.
// switchActive moves a miner now, replaying the target's cached job. Right for
// failover, where the coin it is leaving has gone and there is no work worth
// preserving.
func (m *Mesh) switchActive(s *Session, target *Backend) {
	m.switchTo(s, target, nil)
}

// switchDeferred asks for a move at the target's next job rather than
// immediately. The miner keeps earning on its current coin meanwhile and
// abandons that work on a real job boundary, so nothing in flight is wasted.
// Used for rotation and for a user's reassignment, where a few seconds' delay
// costs nothing and a mid-job move costs every share already computed.
func (m *Mesh) switchDeferred(s *Session, target *Backend) {
	prev := s.activeBackend()
	if target == nil || target == prev || !target.Alive() {
		return
	}
	s.setPending(target)
	m.logger.Info("[nexus] %s: switching %s -> %s at its next job", s.id, symbolOf(prev), target.Symbol)
}

// switchTo performs the move. When notify is non-nil it is the target's own
// freshly arrived job, which is what makes a deferred switch lossless; otherwise
// the target's cached job is replayed.
func (m *Mesh) switchTo(s *Session, target *Backend, notify []byte) {
	prev := s.activeBackend()
	if target == nil || target == prev || !target.Alive() {
		return
	}
	s.setPending(nil)
	// The coin being left stops being live, so its next job reaches the warm-job
	// hook if the miner is later waiting to come back to it.
	if prev != nil {
		prev.GoWarm()
	}

	if !s.supportsExtranonceSub() {
		m.logger.Info("[nexus] %s: switching %s -> %s requires reconnect (no extranonce.subscribe)",
			s.id, symbolOf(prev), target.Symbol)
		s.Close()
		return
	}

	s.setActive(target)

	en1, en2sz := target.Extranonce()
	if err := s.send(map[string]interface{}{
		"id":     nil,
		"method": "mining.set_extranonce",
		"params": []interface{}{en1, en2sz},
	}); err != nil {
		m.logger.Warn("[nexus] %s: set_extranonce failed during switch: %v", s.id, err)
		s.Close()
		return
	}

	setDiff, cached := target.GoLive()
	if notify == nil {
		notify = cached
	}
	if setDiff != nil {
		s.SendRaw(setDiff)
	}
	if notify != nil {
		// Registered here for the same reason as the authorize path: a replayed job
		// bypasses onMessage, and an unregistered job can only be routed by falling
		// back to the active backend.
		if coinJob := notifyJobID(notify); coinJob != "" {
			notify = rewriteNotifyJobID(notify, s.registerJob(target, coinJob))
		}
		s.SendRaw(notify)
	}

	m.logger.Info("[nexus] %s: switched %s -> %s (diff=%t job=%t)",
		s.id, symbolOf(prev), target.Symbol, setDiff != nil, notify != nil)
	m.statSwitches.Add(1)
	m.note(ActivitySwitch, s.workerName(), symbolOf(prev), target.Symbol, "")
}

// authorizeSettle is how long to let a coin's post-authorize difficulty messages
// arrive before replaying one to the miner. Short enough not to delay the miner
// noticeably, long enough to catch a restored difficulty sent immediately after
// the base one.
const authorizeSettle = 750 * time.Millisecond

// symbolOf is nil-safe so switch logging works before a backend is bonded.
func symbolOf(b *Backend) string {
	if b == nil {
		return "none"
	}
	return b.Symbol
}

func (m *Mesh) runMiner(s *Session, backends []*Backend) {
	s.setBonded(backends)
	defer s.Close()
	defer func() {
		// Mark the session closed before its backends go, so their ending reads as
		// the teardown it is rather than as a coin dying under a live miner.
		m.note(ActivityLeave, s.workerName(), symbolOf(s.activeBackend()), "", "")
		s.Close()
		for _, b := range backends {
			b.Close()
		}
	}()

	// The first bonded coin is active; the rest run warm. Every backend reads from
	// its coin continuously so its latest job and difficulty stay current, but only
	// the active one's messages reach the miner. Jobs are registered as they are
	// forwarded, not as they arrive, so the registry only holds jobs the miner has
	// actually been given and could submit against.
	// The highest-priority coin that is actually serving becomes active. Coins can
	// now be bonded dead — configured but not running when the miner connected — so
	// the first entry is not necessarily usable.
	active := firstAlive(backends, nil)
	if active == nil {
		m.logger.Warn("[nexus] %s: no bonded coin is serving; closing so the miner retries", s.id)
		s.Close()
		return
	}
	s.setActive(active)

	for _, b := range backends {
		b := b
		b.onMessage = func(line []byte) {
			// A reply to a share goes back to the miner whichever coin sends it -
			// including the coin it has just left, whose replies were dropped before.
			if m.noteSubmitResponse(s, b.Symbol, line) {
				_ = s.SendRaw(line)
				return
			}
			if s.activeBackend() != b {
				return
			}
			// Namespace job IDs before they reach the miner, and remember which backend
			// issued each one, so a submit can be routed back to the coin that owns it.
			if coinJob := notifyJobID(line); coinJob != "" {
				line = rewriteNotifyJobID(line, s.registerJob(b, coinJob))
			}
			if err := s.SendRaw(line); err != nil {
				s.logger.Debug("[nexus] %s miner write failed: %v", s.id, err)
			}
		}
		b.onDead = func() {
			if s.isClosed() {
				return
			}
			// A warm coin dying is tolerated — it is skipped until it comes back. The
			// active coin dying means the miner has nowhere to get work from, so move it
			// to the best coin still alive. Only if none are left is the session closed,
			// letting the miner reconnect rather than hash a job that will never be
			// replaced.
			if s.activeBackend() != b {
				return
			}
			if next := firstAlive(backends, b); next != nil {
				m.logger.Warn("[nexus] %s: active backend %s died; failing over to %s", s.id, b.Symbol, next.Symbol)
				m.switchActive(s, next)
				return
			}
			m.logger.Warn("[nexus] %s: active backend %s died and no bonded coin is alive; closing miner", s.id, b.Symbol)
			s.Close()
		}
		go b.Run()
	}

	// Failback. The coin list is in priority order, so a miner running on a later
	// coin while an earlier one is alive is running on a fallback it no longer needs
	// — which is what happens when a coin is briefly down at bond time, or is still
	// syncing. Without this the miner stays on the fallback until something forces a
	// reconnect.
	// Keep every bonded coin trying to come back while the miner is connected, so a
	// coin that was down at bond time or died since becomes a failback candidate
	// again rather than staying dead for the life of the session.
	for _, b := range backends {
		b := b
		go b.Reconnect(s.isClosed)
	}

	// A warm backend's job is what a deferred switch waits for: when the target
	// sends one, move on that job rather than replaying a cached one.
	for _, wb := range backends {
		b := wb
		b.SetWarmResponseHandler(func(line []byte) {
			if m.noteSubmitResponse(s, b.Symbol, line) {
				_ = s.SendRaw(line)
			}
		})
		b.SetWarmNotifyHandler(func(line []byte) {
			if target, _ := s.pendingSwitch(); target == b {
				m.switchTo(s, b, line)
			}
		})
	}

	go m.failbackLoop(s, backends)
	go m.keepaliveLoop(s)
	// Only does anything once the miner turns out to be rotating; the loop checks
	// its weights each tick, so a miner assigned a split later starts rotating
	// without reconnecting.
	if m.opts.RotateCycle != nil {
		go m.rotateLoop(s, backends, m.opts.RotateCycle())
	}

	// The active backend is looked up per message rather than captured: it changes
	// when a coin dies or a higher-priority one comes back, and a stale capture
	// would keep forwarding to the coin the miner has already been moved off.

	for {
		line, err := s.readLine()
		if err != nil {
			s.logger.Debug("[nexus] %s miner read end: %v", s.id, err)
			return
		}
		var msg rpcMsg
		if err := json.Unmarshal(line, &msg); err != nil {
			s.logger.Debug("[nexus] %s bad miner json: %s", s.id, string(line))
			continue
		}

		switch msg.Method {
		case "mining.configure":
			b := s.activeBackend()
			// Answer the miner's version-rolling request with the mask the coin
			// actually granted this backend (see Backend.Connect). Handling it here
			// (not forwarding) keeps the miner's rolled version bits within the
			// coin's accepted mask so its shares validate.
			mask := b.VersionMask()
			if mask == "" {
				mask = "1fffe000" // BIP320 standard fallback
			}
			s.send(map[string]interface{}{
				"id": rawOrNull(msg.ID),
				"result": map[string]interface{}{
					"version-rolling":      true,
					"version-rolling.mask": mask,
				},
				"error": nil,
			})

		case "mining.subscribe":
			// The first param is the miner's client string (e.g. "bitaxe/2.9.0").
			// The coin never sees it, since the relay answers subscribe itself.
			var subParams []string
			if err := json.Unmarshal(msg.Params, &subParams); err == nil && len(subParams) > 0 {
				s.setVendor(subParams[0])
			}
			b := s.activeBackend()
			en1, en2sz := b.Extranonce()
			resp := map[string]interface{}{
				"id": rawOrNull(msg.ID),
				"result": []interface{}{
					[]interface{}{
						[]interface{}{"mining.set_difficulty", "nexus1"},
						[]interface{}{"mining.notify", "nexus1"},
					},
					en1, en2sz,
				},
				"error": nil,
			}
			if err := s.send(resp); err != nil {
				return
			}

		case "mining.authorize":
			var params []string
			_ = json.Unmarshal(msg.Params, &params)
			worker := ""
			if len(params) > 0 {
				worker = params[0]
			}
			s.setWorker(worker)
			m.registerLive(workerSuffix(worker), s)
			defer m.unregisterLive(workerSuffix(worker), s)

			// A miner the user has assigned to a coin starts on that coin rather than
			// whichever the bond order picked. Resolved here because authorize is the
			// first point the worker name is known — bonding happens before the miner
			// says who it is. An assignment naming a coin that is down is left alone:
			// the miner mines what is available and the failback loop moves it across
			// when its coin returns.
			if m.opts.Assignment != nil && worker != "" {
				if weights, ok := m.opts.Assignment(workerSuffix(worker)); ok && len(weights) > 0 {
					// More than one weight means the user wants this miner's time split
					// across coins. There is no single coin to call home in that case, so
					// the rotation loop drives it and failback stays out of the way.
					if len(weights) > 1 {
						s.setRotation(weights)
						m.logger.Info("[nexus] %s: %s rotating across %s", s.id, worker, describeWeights(weights))
						goto rotationSet
					}
					sym := weights[0].Coin
					for _, ab := range backends {
						if !strings.EqualFold(ab.Symbol, sym) {
							continue
						}
						if ab == s.activeBackend() {
							s.setHome(ab)
							break
						}
						if !ab.Alive() {
							// Home is recorded even though we cannot go there yet: the
							// reconnect loop will bring the coin back and the failback
							// ticker will move the miner across without it reconnecting.
							s.setHome(ab)
							m.logger.Info("[nexus] %s: %s assigned to %s, which is down; starting on %s until it returns",
								s.id, worker, ab.Symbol, symbolOf(s.activeBackend()))
							break
						}
						m.logger.Info("[nexus] %s: %s assigned to %s; starting there", s.id, worker, ab.Symbol)
						s.setActive(ab)
						s.setHome(ab)
						break
					}
				}
			}
		rotationSet:

			b := s.activeBackend()

			// Authorize every bonded backend using the MINER's own worker name, so each
			// coin tracks a stable identity across reconnects. Warm coins must be
			// authorized too: a coin only starts sending jobs after authorize, and a
			// warm backend with no cached job would have nothing to hand the miner when
			// rotation switches to it. Run() is already reading on each, so responses,
			// difficulty and first jobs arrive asynchronously.
			for _, wb := range backends {
				if err := wb.Authorize(worker); err != nil {
					m.logger.Warn("[nexus] %s: backend %s authorize failed: %v", s.id, wb.Symbol, err)
					if wb == b {
						return
					}
				}
			}
			// Only the active coin gates the miner's authorize response: warm coins can
			// take their time producing a first job without holding the miner up.
			if !b.WaitForFirstJob(10 * time.Second) {
				m.logger.Warn("[nexus] %s: no job from %s within 10s; closing", s.id, b.Symbol)
				return
			}
			if err := s.send(map[string]interface{}{
				"id": rawOrNull(msg.ID), "result": true, "error": nil,
			}); err != nil {
				return
			}
			// Flip the backend live and replay the current difficulty + latest job
			// so the miner starts immediately — no race, no waiting for the coin's
			// next template refresh.
			// A coin sends its base difficulty on authorize and then, a moment
			// later, the difficulty it remembered for this worker from its last
			// session. Replaying the cache the instant the miner authorizes catches
			// only the first of those, so the miner starts work at the base while
			// the coin already expects the restored value — and every share from
			// that work is rejected as low-difficulty. Waiting for the burst to
			// settle costs a fraction of a second and means the miner is told the
			// difficulty the coin is actually holding it to.
			b.SettleDifficulty(authorizeSettle)

			setDiff, notify := b.GoLive()
			if setDiff != nil {
				s.SendRaw(setDiff)
			}
			if notify != nil {
				// The replayed job bypasses onMessage, so namespace and register it here
				// too. Without this the miner mines a job Nexus has no record of, and its
				// submit can only be routed by falling back to the active backend — which
				// is the wrong coin as soon as rotation has moved on.
				if coinJob := notifyJobID(notify); coinJob != "" {
					notify = rewriteNotifyJobID(notify, s.registerJob(b, coinJob))
				}
				s.SendRaw(notify)
			}
			m.logger.Info("[nexus] %s miner authorized (worker=%q) -> bonded to %s (replayed diff=%t job=%t)",
				s.id, worker, b.Symbol, setDiff != nil, notify != nil)
			m.note(ActivityJoin, worker, "", b.Symbol, "")

		case "mining.extranonce.subscribe":
			s.mu.Lock()
			s.supportsXnSub = true
			s.mu.Unlock()
			m.logger.Info("[nexus] %s: miner supports extranonce subscribe (seamless coin switching available)", s.id)
			s.send(map[string]interface{}{"id": rawOrNull(msg.ID), "result": true, "error": nil})

		case "mining.submit":
			b := s.activeBackend()
			// Route the submit to the backend that issued this job, not merely the
			// active one: after a coin switch, work returned for the previous coin
			// must still reach it. Falls back to the active backend if the job has
			// aged out of the registry.
			target := b
			out := line
			if nexusJob := submitJobID(line); nexusJob != "" {
				if owner, coinJob, ok := s.lookupJob(nexusJob); ok {
					target = owner
					out = rewriteSubmitJobID(out, coinJob)
				} else {
					// A share for a job this connection never issued - work from before a
					// reconnect or an engine restart. No coin can accept it, so it is answered
					// here as stale rather than passed on to be rejected, and counted in the
					// mesh's own tally so nothing is hidden.
					m.logger.Info("[nexus] %s: share for job %s, which this connection never issued; answered as stale", s.id, nexusJob)
					var sub struct {
						ID json.RawMessage `json:"id"`
					}
					_ = json.Unmarshal(line, &sub)
					_ = s.send(map[string]interface{}{"id": rawOrNull(sub.ID), "result": nil, "error": []interface{}{21, "Job not found (stale)", nil}})
					m.statLost.Add(1)
					m.tallyOn(s.workerName(), "", 3)
					break
				}
			}
			out = rewriteSubmitWorker(out, target.Worker)
			var sub struct {
				ID json.RawMessage `json:"id"`
			}
			_ = json.Unmarshal(line, &sub)
			s.addPendingSubmit(string(sub.ID))
			if err := target.SendRaw(out); err != nil {
				m.logger.Info("[nexus] %s submit forward FAILED: %v", s.id, err)
			} else {
				s.recordShare(target.Difficulty())
				m.logger.Debug("[nexus] %s submit forwarded to %s: %s", s.id, target.Symbol, truncate(string(out), 160))
			}

		default:
			s.activeBackend().SendRaw(line)
		}
	}
}

func rawOrNull(id json.RawMessage) interface{} {
	if len(id) == 0 {
		return nil
	}
	return id
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// rewriteNotifyJobID replaces params[0] (the job ID) in a mining.notify with the
// Nexus-issued ID, so IDs from different coins can't collide on the miner side.
// Returns the line unchanged if it isn't a well-formed notify.
func rewriteNotifyJobID(line []byte, jobID string) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(line, &m); err != nil {
		return line
	}
	params, ok := m["params"].([]interface{})
	if !ok || len(params) < 1 {
		return line
	}
	params[0] = jobID
	m["params"] = params
	out, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return out
}

// notifyJobID extracts params[0] from a mining.notify, or "" if the line is not
// a notify. Checks the method explicitly rather than inferring from the shape of
// params, so other server-to-miner messages can never be mistaken for jobs.
func notifyJobID(line []byte) string {
	var m map[string]interface{}
	if err := json.Unmarshal(line, &m); err != nil {
		return ""
	}
	if method, _ := m["method"].(string); method != "mining.notify" {
		return ""
	}
	params, ok := m["params"].([]interface{})
	if !ok || len(params) < 1 {
		return ""
	}
	id, _ := params[0].(string)
	return id
}

// rewriteSubmitJobID replaces params[1] (the job ID) in a mining.submit with the
// coin's own ID for that job, undoing the Nexus namespacing before forwarding.
func rewriteSubmitJobID(line []byte, coinJob string) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(line, &m); err != nil {
		return line
	}
	params, ok := m["params"].([]interface{})
	if !ok || len(params) < 2 {
		return line
	}
	params[1] = coinJob
	m["params"] = params
	out, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return out
}

// submitJobID extracts params[1] from a mining.submit, or "" if absent.
func submitJobID(line []byte) string {
	var m map[string]interface{}
	if err := json.Unmarshal(line, &m); err != nil {
		return ""
	}
	params, ok := m["params"].([]interface{})
	if !ok || len(params) < 2 {
		return ""
	}
	id, _ := params[1].(string)
	return id
}

// rewriteSubmitWorker replaces params[0] (the worker name) in a mining.submit
// line with the given worker, preserving all other fields.
func rewriteSubmitWorker(line []byte, worker string) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(line, &m); err != nil {
		return line
	}
	params, ok := m["params"].([]interface{})
	if !ok || len(params) < 1 {
		return line
	}
	params[0] = worker
	m["params"] = params
	out, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return out
}
