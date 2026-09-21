package mesh

import (
	"bufio"
	"encoding/json"
	"net"
	"strconv"
	"strings"
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

	mu     sync.Mutex
	worker string
	active *Backend

	// home is the coin this miner belongs on when everything is available — its
	// assigned coin if the user set one, otherwise the first in the configured
	// order. Failback returns it here. Without this, an assignment was undone
	// within a ticker interval: the miner would start on its assigned coin and the
	// failback loop would immediately pull it back to whatever came first in the
	// config, because list position was the only notion of priority.
	home          *Backend
	supportsXnSub bool

	// bonded is every backend this session holds, in configured order. Kept here so
	// a reassignment arriving from the API can find the target coin — the relay
	// goroutine's locals are not reachable from outside it.
	bonded []*Backend

	// rotation is the miner's weighted split across coins, when the user gave it
	// more than one. A rotating miner has no home coin — the schedule decides where
	// it should be — so failback must leave it alone or the two fight, each undoing
	// the other within a ticker interval.
	rotation []Weight

	// The miner's own address and the client string it subscribed with. The coin
	// behind the relay sees only the relay's connection, so these are the only place
	// the real values exist — without them a meshed miner shows as 127.0.0.1 with no
	// hardware, which is the relay's view rather than anything useful.
	remoteAddr string
	vendor     string

	// pending is a switch waiting for its target's next job. Moving a miner
	// mid-job gives it a new difficulty while work computed at the old one is
	// still in flight, and the coin it lands on rejects all of it — badly so
	// between coins of very different difficulty. Deferring until the target
	// sends its own job means the miner abandons the old one exactly as it would
	// on a new block. pendingSince bounds the wait: a quiet coin must not hold a
	// miner off its allocation indefinitely.
	pending      *Backend
	pendingSince time.Time

	// shares is a rolling record of submitted work, for measuring the miner's
	// hashrate at the relay. Unlike a coin's own average it follows the miner
	// across switches, since every share passes through here whichever coin it
	// is for.
	shares []shareSample
	closed bool

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
	// The miner's address, captured here because the coin behind the relay only
	// ever sees the relay's own connection.
	addr := ""
	if conn != nil && conn.RemoteAddr() != nil {
		addr = conn.RemoteAddr().String()
		if i := strings.LastIndex(addr, ":"); i >= 0 {
			addr = addr[:i]
		}
	}
	return &Session{id: id, conn: conn, reader: bufio.NewReader(conn), logger: logger, remoteAddr: addr}
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

// isClosed reports whether the miner connection has been torn down, so background
// loops tied to this session can stop.
func (s *Session) isClosed() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed }

func (s *Session) setWorker(w string)   { s.mu.Lock(); s.worker = w; s.mu.Unlock() }
func (s *Session) setActive(b *Backend) { s.mu.Lock(); s.active = b; s.mu.Unlock() }

// shareSample is one submitted share: when, and at what difficulty.
type shareSample struct {
	at   time.Time
	diff float64
}

// hashrateWindow is how far back the relay looks when measuring a miner.
const hashrateWindow = 10 * time.Minute

// recordShare notes a submitted share at the difficulty it was mined against.
func (s *Session) recordShare(diff float64) {
	if diff <= 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shares = append(s.shares, shareSample{at: now, diff: diff})
	cut := 0
	for cut < len(s.shares) && now.Sub(s.shares[cut].at) > hashrateWindow {
		cut++
	}
	s.shares = s.shares[cut:]
}

// MeasuredHashrate returns the miner's hashrate in H/s from the work it has
// submitted, and how many shares that figure rests on. Every share of
// difficulty D represents about D x 2^32 hashes. Early in a session the window
// is short and the figure noisy — which is why a miner's own reported
// hashrate is preferred whenever it can be read.
func (s *Session) MeasuredHashrate() (hps float64, samples int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.shares) < 2 {
		return 0, len(s.shares)
	}
	span := time.Since(s.shares[0].at)
	if span < time.Minute {
		span = time.Minute
	}
	var total float64
	for _, x := range s.shares {
		total += x.diff
	}
	return total * 4294967296 / span.Seconds(), len(s.shares)
}

// setPending records a switch waiting for the target's next job, or clears it
// when passed nil.
func (s *Session) setPending(b *Backend) {
	s.mu.Lock()
	s.pending = b
	s.pendingSince = time.Now()
	s.mu.Unlock()
}

// pendingSwitch returns the deferred target and how long it has been waiting.
func (s *Session) pendingSwitch() (*Backend, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		return nil, 0
	}
	return s.pending, time.Since(s.pendingSince)
}

// setVendor records the client string the miner subscribed with.
func (s *Session) setVendor(v string) { s.mu.Lock(); s.vendor = v; s.mu.Unlock() }

// Facts returns the miner's own address and client string, for display.
func (s *Session) Facts() (remoteAddr, vendor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteAddr, s.vendor
}

// setRotation records a miner's weighted split. Empty means it does not rotate.
func (s *Session) setRotation(w []Weight) { s.mu.Lock(); s.rotation = w; s.mu.Unlock() }

// rotationWeights returns the miner's split, or nil if it does not rotate.
func (s *Session) rotationWeights() []Weight {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rotation) == 0 {
		return nil
	}
	out := make([]Weight, len(s.rotation))
	copy(out, s.rotation)
	return out
}

// setBonded records the backends this session was bonded to.
func (s *Session) setBonded(b []*Backend) { s.mu.Lock(); s.bonded = b; s.mu.Unlock() }

// bondedBackends returns a copy of this session's backends.
func (s *Session) bondedBackends() []*Backend {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Backend, len(s.bonded))
	copy(out, s.bonded)
	return out
}

// setHome records the coin this session should return to when it is available.
func (s *Session) setHome(b *Backend) { s.mu.Lock(); s.home = b; s.mu.Unlock() }

// homeBackend returns the session's home coin, or nil if none was set.
func (s *Session) homeBackend() *Backend {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.home
}

// supportsExtranonceSub reports whether the miner asked for extranonce updates.
// Only such miners can be moved between coins without reconnecting: the coins hand
// out different extranonce1 values, and a miner that cannot be told its extranonce
// changed would build shares the new coin rejects.
func (s *Session) supportsExtranonceSub() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.supportsXnSub
}

// activeBackend returns the coin backend currently feeding this miner. Warm
// backends compare unequal to it and so hold their jobs back.
func (s *Session) activeBackend() *Backend {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

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
