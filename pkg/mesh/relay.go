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
		go b.Run()
	}

	b := active

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
			b.SendRaw(line)
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
