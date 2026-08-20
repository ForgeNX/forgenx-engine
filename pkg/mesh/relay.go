package mesh

import (
	"encoding/json"
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
func (m *Mesh) switchActive(s *Session, target *Backend) {
	prev := s.activeBackend()
	if target == nil || target == prev || !target.Alive() {
		return
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

	setDiff, notify := target.GoLive()
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
}

// symbolOf is nil-safe so switch logging works before a backend is bonded.
func symbolOf(b *Backend) string {
	if b == nil {
		return "none"
	}
	return b.Symbol
}

func (m *Mesh) runMiner(s *Session, backends []*Backend) {
	defer s.Close()
	defer func() {
		for _, b := range backends {
			b.Close()
		}
	}()

	// The first bonded coin is active; the rest run warm. Every backend reads from
	// its coin continuously so its latest job and difficulty stay current, but only
	// the active one's messages reach the miner. Jobs are registered as they are
	// forwarded, not as they arrive, so the registry only holds jobs the miner has
	// actually been given and could submit against.
	active := backends[0]
	s.setActive(active)

	for _, b := range backends {
		b := b
		b.onMessage = func(line []byte) {
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

	go m.failbackLoop(s, backends)

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
			b := s.activeBackend()
			var params []string
			_ = json.Unmarshal(msg.Params, &params)
			worker := ""
			if len(params) > 0 {
				worker = params[0]
			}
			s.setWorker(worker)

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
					m.logger.Warn("[nexus] %s: submit for unknown job %s; forwarding to active backend", s.id, nexusJob)
				}
			}
			out = rewriteSubmitWorker(out, target.Worker)
			if err := target.SendRaw(out); err != nil {
				m.logger.Info("[nexus] %s submit forward FAILED: %v", s.id, err)
			} else {
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
