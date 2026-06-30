package stratumv2

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// SV2 Server
//
// The Server:
//   1. Listens on a TCP port (default: 3335, separate from V1's 3334).
//   2. Performs the Noise_NX handshake on each accepted connection.
//   3. Wraps it in a Session and runs it in a goroutine.
//   4. Exposes BroadcastTemplate() which the engine calls whenever the
//      ZMQ block-template subscriber fires.
//   5. Accepts a shareSubmitCallback so block solutions reach the node RPC.
//
// Integration with the existing engine:
//   The engine's coinrunner.go or jobmanager.go calls:
//     sv2Server.BroadcastTemplate(tmpl)
//   whenever a new getblocktemplate response arrives.
//
//   When a miner solves a block:
//     onShare callback → engine's submitblock RPC call
// ──────────────────────────────────────────────────────────────────────────────

// CoinbaseBuilderFunc builds a coinbase transaction pair for a specific
// miner identity. Only used in solo mode, where each worker's block reward
// must pay out to that worker's own address rather than a shared pool
// address. The engine supplies this closure (wrapping pkg/coin's
// BuildCoinbase against the JobManager's current template) so pkg/stratumv2
// never needs to import pkg/coin directly.
//
// userIdentity is the raw string from the miner's OpenStandardMiningChannel
// (typically "address" or "address.workerName" — same convention as V1's
// mining.authorize). Implementations should parse out the address portion
// the same way coinrunner.go's AuthorizeHandler does for V1.
type CoinbaseBuilderFunc func(userIdentity string) (coinbase1, coinbase2 []byte, err error)

// Config holds all tunable parameters for the SV2 server.
type Config struct {
	// ListenAddr is the TCP address to listen on, e.g. ":3335".
	ListenAddr string

	// StaticKeypair is the server's long-lived secp256k1 identity.
	// Generate once with GenerateStaticKeypair() and persist PrivKeyBytes().
	StaticKeypair *StaticKeypair

	// OnShare is called (in a goroutine) for every accepted share.
	// If the share's ShareResult.MeetsBlock is true, the engine should
	// submit the block to the node via RPC.
	OnShare shareSubmitCallback

	// CoinbaseBuilder, if set, enables solo mode: each channel's coinbase
	// is built individually from its UserIdentity instead of using the
	// JobTemplate's shared Coinbase1/Coinbase2. Leave nil for pool mode.
	CoinbaseBuilder CoinbaseBuilderFunc

	// CoinTicker is a short string like "BCH" used in log messages.
	CoinTicker string
}

// Server is the SV2 Mining Protocol server.
type Server struct {
	cfg      Config
	listener net.Listener

	mu       sync.RWMutex
	sessions map[string]*Session // sessionID → Session

	// Job counter — monotonically increasing job IDs.
	jobCounter uint32

	// Shutdown signalling.
	quit chan struct{}
	once sync.Once

	// Statistics (atomic).
	totalConnections  uint64
	activeConnections int64
}

// NewServer creates a new SV2 Server with the given config.
// Call Start() to begin accepting connections.
func NewServer(cfg Config) (*Server, error) {
	if cfg.StaticKeypair == nil {
		return nil, fmt.Errorf("sv2: StaticKeypair is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":3335"
	}
	if cfg.CoinTicker == "" {
		cfg.CoinTicker = "COIN"
	}
	return &Server{
		cfg:      cfg,
		sessions: make(map[string]*Session),
		quit:     make(chan struct{}),
	}, nil
}

// Start begins listening for SV2 miner connections.
// Blocks until Stop() is called or the listener fails.
func (srv *Server) Start() error {
	ln, err := net.Listen("tcp", srv.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("sv2 listen %s: %w", srv.cfg.ListenAddr, err)
	}
	srv.listener = ln

	pubHex := hex.EncodeToString(srv.cfg.StaticKeypair.ellSwiftPub[:])
	log.Printf("[sv2-%s] server listening on %s", srv.cfg.CoinTicker, srv.cfg.ListenAddr)
	log.Printf("[sv2-%s] server public key (EllSwift): %s", srv.cfg.CoinTicker, pubHex)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-srv.quit:
				return nil // clean shutdown
			default:
				log.Printf("[sv2-%s] accept error: %v", srv.cfg.CoinTicker, err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		go srv.handleConn(conn)
	}
}

// Stop gracefully shuts down the server and all active sessions.
func (srv *Server) Stop() {
	srv.once.Do(func() {
		close(srv.quit)
		if srv.listener != nil {
			srv.listener.Close()
		}
		srv.mu.Lock()
		for _, sess := range srv.sessions {
			sess.Close()
		}
		srv.mu.Unlock()
		log.Printf("[sv2-%s] server stopped", srv.cfg.CoinTicker)
	})
}

// handleConn runs the full lifecycle for one inbound TCP connection.
func (srv *Server) handleConn(conn net.Conn) {
	atomic.AddUint64(&srv.totalConnections, 1)
	atomic.AddInt64(&srv.activeConnections, 1)
	defer atomic.AddInt64(&srv.activeConnections, -1)

	remote := conn.RemoteAddr().String()
	log.Printf("[sv2-%s] new connection from %s", srv.cfg.CoinTicker, remote)

	// Noise handshake.
	encConn, err := PerformServerHandshake(conn, srv.cfg.StaticKeypair)
	if err != nil {
		log.Printf("[sv2-%s] handshake failed from %s: %v", srv.cfg.CoinTicker, remote, err)
		conn.Close()
		return
	}

	sess := newSession(encConn, srv.cfg.OnShare, srv.cfg.CoinbaseBuilder)

	srv.mu.Lock()
	srv.sessions[sess.ID()] = sess
	srv.mu.Unlock()

	defer func() {
		srv.mu.Lock()
		delete(srv.sessions, sess.ID())
		srv.mu.Unlock()
	}()

	// Push the most recent template so the miner can start immediately.
	// (Session.handleOpenChannel also does this, but belt-and-suspenders.)
	// Nothing to push here yet; template arrives via BroadcastTemplate.

	sess.Run() // blocks until disconnect
}

// ──────────────────────────────────────────────────────────────────────────────
// Work Dispatch
// ──────────────────────────────────────────────────────────────────────────────

// BroadcastTemplate pushes a new block template to all connected miners.
// This is called by the engine whenever the ZMQ hashblock/hashtx subscriber
// detects a new template is available.
//
// The caller provides the raw block template components; this function
// handles job ID assignment and the SV2 job+prevhash encoding.
func (srv *Server) BroadcastTemplate(tmpl *JobTemplate) {
	// Assign a fresh job ID.
	tmpl.JobID = atomic.AddUint32(&srv.jobCounter, 1)

	srv.mu.RLock()
	sessions := make([]*Session, 0, len(srv.sessions))
	for _, sess := range srv.sessions {
		sessions = append(sessions, sess)
	}
	srv.mu.RUnlock()

	if len(sessions) == 0 {
		return
	}

	log.Printf("[sv2-%s] broadcasting job %d to %d session(s) height=%d",
		srv.cfg.CoinTicker, tmpl.JobID, len(sessions), tmpl.Height)

	for _, sess := range sessions {
		// Each session gets its own goroutine to avoid one slow miner
		// blocking the broadcast to all others.
		go sess.UpdateTemplate(tmpl)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Stats
// ──────────────────────────────────────────────────────────────────────────────

// ServerStats is a snapshot of server-level metrics.
type ServerStats struct {
	ActiveSessions    int64
	TotalConnections  uint64
	TotalChannels     int
	PublicKeyEllSwift string
}

// Stats returns a snapshot of the server's current metrics.
func (srv *Server) Stats() ServerStats {
	srv.mu.RLock()
	totalChannels := 0
	for _, sess := range srv.sessions {
		totalChannels += sess.ChannelCount()
	}
	srv.mu.RUnlock()

	return ServerStats{
		ActiveSessions:    atomic.LoadInt64(&srv.activeConnections),
		TotalConnections:  atomic.LoadUint64(&srv.totalConnections),
		TotalChannels:     totalChannels,
		PublicKeyEllSwift: hex.EncodeToString(srv.cfg.StaticKeypair.ellSwiftPub[:]),
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Engine Integration Helpers
//
// These functions bridge between the engine's existing data types
// (as used in pkg/engine and pkg/coin) and the SV2 JobTemplate struct.
// They live here rather than in the engine package to avoid a circular import.
// ──────────────────────────────────────────────────────────────────────────────

// PrevHashFromHex converts a hex block hash string (display byte order)
// to the internal byte order [32]byte expected by SV2.
// Bitcoin display hashes are reversed (big-endian display of LE storage).
func PrevHashFromHex(hexHash string) ([32]byte, error) {
	var result [32]byte
	b, err := hex.DecodeString(hexHash)
	if err != nil {
		return result, fmt.Errorf("sv2 PrevHashFromHex: %w", err)
	}
	if len(b) != 32 {
		return result, fmt.Errorf("sv2 PrevHashFromHex: expected 32 bytes, got %d", len(b))
	}
	// Reverse from display order to internal order.
	for i := 0; i < 32; i++ {
		result[i] = b[31-i]
	}
	return result, nil
}

// MerkleBranchFromHexSlice converts a []string of hex hashes (from
// getblocktemplate's "transactions" merkle field) to [][32]byte internal order.
func MerkleBranchFromHexSlice(hexHashes []string) ([][32]byte, error) {
	branch := make([][32]byte, len(hexHashes))
	for i, h := range hexHashes {
		b, err := hex.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("sv2 MerkleBranchFromHexSlice[%d]: %w", i, err)
		}
		if len(b) != 32 {
			return nil, fmt.Errorf("sv2 MerkleBranchFromHexSlice[%d]: expected 32 bytes", i)
		}
		copy(branch[i][:], b)
		// Merkle branch hashes from GBT are in display order; reverse to internal.
		reverseBytes(branch[i][:])
	}
	return branch, nil
}

// CoinbaseHashForTemplate computes the double-SHA256 of the concatenated
// coinbase bytes (coinbase1 + en1 + en2 + coinbase2) for a given extranonce pair.
// Exported for use by the engine when submitting a block solution.
func CoinbaseHashForTemplate(coinbase1, en1, en2, coinbase2 []byte) [32]byte {
	return HashCoinbaseTx(coinbase1, en1, en2, coinbase2)
}

// AssembleBlockHex builds a complete serialised block ready for submitblock RPC.
// header (80 bytes) + txcount varint + coinbase tx + other transactions
//
// This is a thin wrapper that the engine must complete — it receives the
// raw transaction bytes from getblocktemplate and prepends the solved header.
func AssembleBlockHex(header [80]byte, coinbaseTx []byte, otherTxs [][]byte) string {
	var block []byte
	block = append(block, header[:]...)
	block = append(block, encodeVarInt(uint64(1+len(otherTxs)))...)
	block = append(block, coinbaseTx...)
	for _, tx := range otherTxs {
		block = append(block, tx...)
	}
	return hex.EncodeToString(block)
}

// encodeVarInt encodes a Bitcoin variable-length integer.
func encodeVarInt(n uint64) []byte {
	switch {
	case n < 0xFD:
		return []byte{byte(n)}
	case n <= 0xFFFF:
		b := make([]byte, 3)
		b[0] = 0xFD
		binary.LittleEndian.PutUint16(b[1:], uint16(n))
		return b
	case n <= 0xFFFFFFFF:
		b := make([]byte, 5)
		b[0] = 0xFE
		binary.LittleEndian.PutUint32(b[1:], uint32(n))
		return b
	default:
		b := make([]byte, 9)
		b[0] = 0xFF
		binary.LittleEndian.PutUint64(b[1:], n)
		return b
	}
}

// merkleRootFromBranch replicates ComputeMerkleRoot for use by the engine
// without importing pow.go symbols (though they're in the same package).
// Left here as documentation of the call site in coinrunner.go.
func merkleRootFromBranch(coinbaseTxHash [32]byte, branch [][32]byte) [32]byte {
	return ComputeMerkleRoot(coinbaseTxHash, branch)
}

// doublesha256 is a convenience wrapper (also available as HashBlockHeader
// for the full 80-byte case).
func doublesha256(b []byte) [32]byte {
	first := sha256.Sum256(b)
	return sha256.Sum256(first[:])
}
