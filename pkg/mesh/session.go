package mesh

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
)

// Session represents one miner connected to Nexus. It holds the miner's front
// connection and the currently-bonded coin backend. Single-coin for now;
// rotation (added later) swaps which backend is active.
type Session struct {
	id     string
	conn   net.Conn
	reader *bufio.Reader
	logger *logging.Logger

	mu            sync.Mutex
	worker        string
	active        *Backend
	supportsXnSub bool
	closed        bool
}

func NewSession(id string, conn net.Conn, logger *logging.Logger) *Session {
	return &Session{id: id, conn: conn, reader: bufio.NewReader(conn), logger: logger}
}

func (s *Session) SendRaw(line []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return net.ErrClosed
	}
	s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := s.conn.Write(append(line, '\n'))
	return err
}

func (s *Session) send(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.SendRaw(data)
}

func (s *Session) readLine() ([]byte, error) {
	line, err := s.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line, nil
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		if s.conn != nil {
			s.conn.Close()
		}
	}
}

func (s *Session) setWorker(w string)   { s.mu.Lock(); s.worker = w; s.mu.Unlock() }
func (s *Session) setActive(b *Backend) { s.mu.Lock(); s.active = b; s.mu.Unlock() }
