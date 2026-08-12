package mesh

import (
	"fmt"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// MinerSession is the mesh's per-miner state: the underlying stratum session
// plus the mesh-specific allocation and bookkeeping. One miner connects to the
// mesh once and is represented by exactly one MinerSession, regardless of how
// many coins its hashrate is split across.
type MinerSession struct {
	// Session is the underlying validated stratum session (handshake, notify,
	// submit all handled by pkg/stratum, exactly as for a normal coin).
	Session *stratum.Session

	// Worker is the miner's authorized worker name (identifier only — the mesh
	// substitutes each coin's configured payout address for the coinbase, so the
	// address part of the miner's username is not used for payout).
	Worker string

	// ConnectedAt is when the miner authorized on the mesh.
	ConnectedAt time.Time

	// alloc holds this miner's coin allocation (percentages, pins). Filled in by
	// the scheduler layer; nil until an allocation is set.
	alloc *Allocation
}

// Listener is the mesh's single Stratum V1 endpoint. Miners connect here once;
// the scheduler then time-slices each miner across its allocated coins. The
// listener owns the stratum server and the set of live miner sessions, and
// exposes hooks the scheduler/router populate.
type Listener struct {
	server *stratum.Server
	logger *logging.Logger
	addr   string

	mu       sync.RWMutex
	sessions map[string]*MinerSession // keyed by stratum session ID

	// onShare is invoked for every share a mesh miner submits. The share router
	// sets this to route the share (by job-ID) to the correct coin's validator.
	// Until the router is wired, a default handler logs and accepts.
	onShare func(ms *MinerSession, share *stratum.ShareSubmission) error

	// onAuthorized is invoked when a miner authorizes on the mesh. The scheduler
	// sets this to begin allocating/rotating jobs for the new miner.
	onAuthorized func(ms *MinerSession)

	// onRemoved is invoked when a miner disconnects, so the scheduler can stop
	// rotating for it and release its allocation.
	onRemoved func(ms *MinerSession)
}

// Config configures the mesh listener.
type Config struct {
	Host           string
	Port           int
	ExtraNonceSize int
	// DefaultDiff is the starting per-miner difficulty before split vardiff tunes
	// it to the miner's measured hashrate.
	DefaultDiff float64
}

// NewListener creates the mesh listener. It does not start listening until
// Start is called. The scheduler and share router register their hooks via the
// SetOn* methods before Start.
func NewListener(cfg Config) *Listener {
	l := &Listener{
		logger:   logging.New(logging.ModuleStratum),
		sessions: make(map[string]*MinerSession),
		addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}

	// Default no-op hooks so the listener is safe to run before the scheduler and
	// router are wired: shares are accepted-and-logged, authorize/remove just log.
	l.onShare = func(ms *MinerSession, share *stratum.ShareSubmission) error {
		l.logger.Debug("[mesh] share from %s (no router wired; accepting)", ms.Worker)
		return nil
	}
	l.onAuthorized = func(ms *MinerSession) {
		l.logger.Info("[mesh] miner authorized: %s (no scheduler wired yet)", ms.Worker)
	}
	l.onRemoved = func(ms *MinerSession) {
		l.logger.Info("[mesh] miner removed: %s", ms.Worker)
	}

	serverCfg := stratum.ServerConfig{
		Host:              cfg.Host,
		Port:              cfg.Port,
		ExtraNonceSize:    cfg.ExtraNonceSize,
		DefaultDiff:       cfg.DefaultDiff,
		AcceptSuggestDiff: true,
		PingEnabled:       true,
		PingInterval:      30 * time.Second,
		// The mesh keeps miners warm by continuously rotating jobs, so the idle
		// timeout only needs to catch genuinely dead sockets.
		IdleTimeout: 10 * time.Minute,
		// Accept any worker: the mesh does not validate the miner's address (it
		// substitutes each coin's configured payout address per job).
		AuthorizeHandler: func(s *stratum.Session, worker string) (string, error) {
			return worker, nil
		},
		// No initial job on authorize — the scheduler sends the first rotated job.
		JobForSessionHandler: func(s *stratum.Session) *stratum.Job {
			return nil
		},
		OnSessionAuthorized: l.handleAuthorized,
		OnSessionRemoved:    l.handleRemoved,
	}

	l.server = stratum.NewServer(serverCfg, l.handleShare)
	return l
}

// SetOnShare registers the share router's handler.
func (l *Listener) SetOnShare(fn func(ms *MinerSession, share *stratum.ShareSubmission) error) {
	l.onShare = fn
}

// SetOnAuthorized registers the scheduler's new-miner handler.
func (l *Listener) SetOnAuthorized(fn func(ms *MinerSession)) {
	l.onAuthorized = fn
}

// SetOnRemoved registers the scheduler's miner-gone handler.
func (l *Listener) SetOnRemoved(fn func(ms *MinerSession)) {
	l.onRemoved = fn
}

// Start begins listening for miner connections.
func (l *Listener) Start() error {
	l.logger.Info("[mesh] listener starting on %s", l.addr)
	return l.server.Start()
}

// Stop shuts the listener down.
func (l *Listener) Stop() {
	l.server.Stop()
}

// Sessions returns a snapshot of live mesh miner sessions.
func (l *Listener) Sessions() []*MinerSession {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]*MinerSession, 0, len(l.sessions))
	for _, ms := range l.sessions {
		out = append(out, ms)
	}
	return out
}

// handleAuthorized wraps a newly-authorized stratum session in a MinerSession,
// tracks it, and notifies the scheduler.
func (l *Listener) handleAuthorized(s *stratum.Session) {
	ms := &MinerSession{
		Session:     s,
		Worker:      s.WorkerName(),
		ConnectedAt: time.Now(),
	}
	l.mu.Lock()
	l.sessions[s.ID] = ms
	l.mu.Unlock()
	l.onAuthorized(ms)
}

// handleRemoved untracks a disconnected session and notifies the scheduler.
func (l *Listener) handleRemoved(s *stratum.Session) {
	l.mu.Lock()
	ms := l.sessions[s.ID]
	delete(l.sessions, s.ID)
	l.mu.Unlock()
	if ms != nil {
		l.onRemoved(ms)
	}
}

// handleShare looks up the mesh session for the submitting stratum session and
// dispatches to the registered share router.
func (l *Listener) handleShare(s *stratum.Session, share *stratum.ShareSubmission) error {
	l.mu.RLock()
	ms := l.sessions[s.ID]
	l.mu.RUnlock()
	if ms == nil {
		return nil
	}
	return l.onShare(ms, share)
}
