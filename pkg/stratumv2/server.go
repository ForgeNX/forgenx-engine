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

	sess := newSession(encConn, srv.cfg.OnShare)

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

// BuildCoinbase constructs the coinbase transaction split around the extranonce
// insertion point, returning (coinbase1, coinbase2).
//
// For SV2 Standard Channel, extranonce1 (4 bytes, server-assigned) and
// extranonce2 (4 bytes, miner-rolled) are concatenated between coinbase1
// and coinbase2 exactly as in Stratum V1.
//
// Parameters:
//
//	coinbaseValue   — block subsidy + fees in satoshis
//	payoutAddress   — miner's BCH/BTC address (P2PKH/P2SH script)
//	heightBytes     — BIP 34 block height as CScript bytes (little-endian)
//	extraData       — arbitrary bytes appended to coinbase scriptSig (e.g., "ForgeNX")
//	extranonce1Size — always 4 for ForgeNX
//	extranonce2Size — always 4 for ForgeNX
func BuildCoinbase(
	coinbaseValue int64,
	p2pkhScript []byte,
	heightBytes []byte,
	extraData []byte,
	extranonce1Size, extranonce2Size int,
) (coinbase1, coinbase2 []byte) {
	// This is a simplified coinbase builder for solo mining.
	// A full implementation would build a proper Bitcoin transaction;
	// here we produce the bytes that match the node's coinbase template
	// with the extranonce splice point at the scriptSig boundary.

	// txVersion (4 LE) + inputCount (1) + prevout (36) + scriptSig...
	// For solo mining, the engine typically gets coinbase1/coinbase2 directly
	// from getblocktemplate's "coinbasetxn" or builds them from scratch.
	// This stub is a placeholder — the real implementation will call
	// the coin-specific builder (e.g., pkg/coin/bitcoincash.go BuildCoinbaseTx).
	_ = coinbaseValue
	_ = p2pkhScript
	_ = heightBytes
	_ = extraData
	_ = extranonce1Size
	_ = extranonce2Size

	// Return empty stubs — the engine integration in coinrunner.go will
	// supply the actual bytes. This function's signature documents the
	// contract that the engine must fulfill.
	return nil, nil
}

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
