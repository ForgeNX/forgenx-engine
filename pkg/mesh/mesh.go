package mesh

import (
	"fmt"
	"net"
	"strconv"
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
}

func New(opts Options) *Mesh {
	return &Mesh{opts: opts, logger: logging.New(logging.ModuleNexus)}
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
	var backends []*Backend
	for _, symbol := range m.opts.Coins {
		host, port, payout, running, ok := m.opts.Resolve(symbol)
		if !ok {
			m.logger.Warn("[nexus] %s: coin %s not found; skipping", id, symbol)
			continue
		}
		if !running {
			m.logger.Warn("[nexus] %s: coin %s V1 stratum not running; skipping", id, symbol)
			continue
		}
		if payout == "" {
			m.logger.Warn("[nexus] %s: coin %s has no payout configured; skipping", id, symbol)
			continue
		}
		backendAddr := fmt.Sprintf("%s:%d", host, port)
		b := NewBackend(symbol, backendAddr, payout, "", m.logger)
		if err := b.Connect(); err != nil {
			m.logger.Warn("[nexus] %s: backend %s connect failed: %v", id, symbol, err)
			continue
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
		symbols = append(symbols, b.Symbol)
	}
	m.logger.Info("[nexus] %s: miner bonded to %v (active=%s)", id, symbols, backends[0].Symbol)

	m.runMiner(s, backends)
}

func (m *Mesh) Stop() {
	if m.listener != nil {
		m.listener.Close()
	}
}
