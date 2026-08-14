package mesh

import "encoding/json"

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
			m.logger.Info("[nexus] %s miner authorized (worker=%q) -> bonded to %s", s.id, worker, b.Symbol)

		case "mining.extranonce.subscribe":
			s.mu.Lock()
			s.supportsXnSub = true
			s.mu.Unlock()
			s.send(map[string]interface{}{"id": rawOrNull(msg.ID), "result": true, "error": nil})

		case "mining.submit":
			if err := b.SendRaw(line); err != nil {
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
