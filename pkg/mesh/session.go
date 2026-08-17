package mesh

import (
	"bufio"
	"encoding/json"
	"net"
	"strconv"
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

	// Job registry. Job IDs issued by different coins collide (each coin numbers
	// its own jobs from zero), so Nexus hands the miner its own namespaced IDs and
	// maps them back. Submits are then routed to the backend that issued the job —
	// which is what lets work returned after a coin switch still reach the coin it
	// was mined for, instead of being rejected as unknown by whichever coin is
	// currently active.
	jobSeq   uint64
	jobs     map[string]jobRef
	jobOrder []string
}

// jobRef ties a Nexus-issued job ID back to the backend that produced it and the
// coin's own ID for that job.
type jobRef struct {
	backend *Backend
	coinJob string
}

// maxTrackedJobs bounds the registry. Sized to cover several minutes of jobs
// across all bonded coins so late submits still route correctly.
const maxTrackedJobs = 512

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

// registerJob assigns a Nexus-namespaced job ID for a job issued by backend b,
// records the mapping, and evicts the oldest entry once the registry is full.
func (s *Session) registerJob(b *Backend, coinJob string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs == nil {
		s.jobs = make(map[string]jobRef, maxTrackedJobs)
	}
	s.jobSeq++
	id := strconv.FormatUint(s.jobSeq, 16)
	s.jobs[id] = jobRef{backend: b, coinJob: coinJob}
	s.jobOrder = append(s.jobOrder, id)
	if len(s.jobOrder) > maxTrackedJobs {
		delete(s.jobs, s.jobOrder[0])
		s.jobOrder = s.jobOrder[1:]
	}
	return id
}

// lookupJob resolves a Nexus job ID back to its backend and the coin's own ID.
func (s *Session) lookupJob(id string) (*Backend, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.jobs[id]
	if !ok {
		return nil, "", false
	}
	return ref.backend, ref.coinJob, true
}
