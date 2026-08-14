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
	Port        int      // miner-facing listen port (e.g. 3350)
	DefaultCoin string   // single-coin bond target (e.g. "DGB")
	Resolve     Resolver // resolves coin symbol -> endpoint
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
	m.logger.Info("[nexus] Mesh listening on %s (single-coin bond=%s)", addr, m.opts.DefaultCoin)
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

	host, port, payout, running, ok := m.opts.Resolve(m.opts.DefaultCoin)
	if !ok {
		m.logger.Warn("[nexus] %s: coin %s not found; closing", id, m.opts.DefaultCoin)
		s.Close()
		return
	}
	if !running {
		m.logger.Warn("[nexus] %s: coin %s V1 stratum not running; closing", id, m.opts.DefaultCoin)
		s.Close()
		return
	}
	if payout == "" {
		m.logger.Warn("[nexus] %s: coin %s has no payout configured; closing", id, m.opts.DefaultCoin)
		s.Close()
		return
	}

	backendAddr := fmt.Sprintf("%s:%d", host, port)
	b := NewBackend(m.opts.DefaultCoin, backendAddr, payout, "nexus", m.logger)
	if err := b.Connect(); err != nil {
		m.logger.Warn("[nexus] %s: backend connect failed: %v", id, err)
		s.Close()
		return
	}
	m.logger.Info("[nexus] %s: miner bonded to %s via %s", id, m.opts.DefaultCoin, backendAddr)
	m.runMiner(s, b)
}

func (m *Mesh) Stop() {
	if m.listener != nil {
		m.listener.Close()
	}
}
