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
func (m *Mesh) runMiner(s *Session, b *Backend) {
	defer s.Close()
	defer b.Close()

	s.setActive(b)

	b.onMessage = func(line []byte) {
		if err := s.SendRaw(line); err != nil {
			s.logger.Debug("[nexus] %s miner write failed: %v", s.id, err)
		}
	}
	go b.Run()

	// Wait for the backend to capture the coin's first job before serving the
	// miner, so the miner always receives real work on authorize (no stall).
	if !b.WaitForFirstJob(10 * time.Second) {
		s.logger.Warn("[nexus] %s: no job from %s within 10s; closing", s.id, b.Symbol)
		return
	}

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
		s.logger.Info("[nexus] %s <- miner: %s", s.id, truncate(string(line), 200))

		switch msg.Method {
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
				s.SendRaw(notify)
			}
			m.logger.Info("[nexus] %s miner authorized (worker=%q) -> bonded to %s (replayed diff=%t job=%t)",
				s.id, worker, b.Symbol, setDiff != nil, notify != nil)

		case "mining.extranonce.subscribe":
			s.mu.Lock()
			s.supportsXnSub = true
			s.mu.Unlock()
			s.send(map[string]interface{}{"id": rawOrNull(msg.ID), "result": true, "error": nil})

		case "mining.submit":
			// The miner submits with ITS OWN worker name in params[0], but the
			// backend authorized to the coin as b.Worker (e.g. "nexus-n1"). Rewrite
			// params[0] to the backend's worker so the coin attributes the share to
			// the connection it authorized (mismatched names are rejected).
			out := rewriteSubmitWorker(line, b.Worker)
			if err := b.SendRaw(out); err != nil {
				s.logger.Debug("[nexus] %s submit forward failed: %v", s.id, err)
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
