package stratumv2

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// ──────────────────────────────────────────────────────────────────────────────
// Session
//
// One Session corresponds to one miner TCP connection. It owns:
//   • The Noise-encrypted net.Conn
//   • The SV2 frame Codec
//   • Zero or more open Channels
//   • The message dispatch loop
//
// Lifecycle:
//   1. TCP accept → PerformSV2ServerHandshake (sv2noise.go)
//   2. newSession() wraps the encrypted conn
//   3. session.Run() is called in a goroutine — reads frames and dispatches
//   4. When SetupConnection arrives, validate and reply Success/Error
//   5. When OpenStandardMiningChannel arrives, create Channel, send Success
//   6. Server calls session.SendJob() / session.SendPrevHash() to push work
//   7. When SubmitSharesStandard arrives, validate and reply Success/Error
//   8. On disconnect, session.Close() cleans up all channels
// ──────────────────────────────────────────────────────────────────────────────

const (
	// sv2ProtocolVersion is the SV2 Mining Protocol version ForgeNX supports.
	sv2ProtocolVersion uint16 = 2

	// maxChannelsPerSession caps how many Standard Mining Channels one
	// connection may open. Reasonable limit for ASICs (typically 1-4).
	maxChannelsPerSession = 16

	// readDeadlineSeconds is the default per-frame read deadline if no config value
	// is provided. Equivalent to GSS's "Connection Timeout". Configurable via
	// Config.ConnectionTimeoutSeconds so users can increase it for high difficulty.
	defaultReadDeadlineSeconds = 600

	// SetupConnection.flags bits (client → server), per
	// sv2-spec 05-Mining-Protocol.md §5.3.1. We accept all three from
	// clients since ForgeNX always sends standard jobs (satisfies
	// REQUIRES_STANDARD_JOBS) and doesn't reject version-rolling clients
	// (satisfies REQUIRES_VERSION_ROLLING — version is fully client-rolled,
	// we never constrain it). REQUIRES_WORK_SELECTION (SetCustomMiningJob)
	// is not implemented; we don't reject the flag, but the corresponding
	// message would currently go unhandled if a client actually sent it.
	sv2FlagRequiresStandardJobs   uint32 = 1 << 0
	sv2FlagRequiresWorkSelection  uint32 = 1 << 1
	sv2FlagRequiresVersionRolling uint32 = 1 << 2
	sv2ClientFlagsAccepted        uint32 = sv2FlagRequiresStandardJobs | sv2FlagRequiresVersionRolling

	// SetupConnection.Success.flags bits (server → client) — a COMPLETELY
	// DIFFERENT flag namespace from the client's request flags above; an
	// earlier version of this code wrongly echoed back (sc.Flags & mask) as
	// if the two shared meaning. We don't set REQUIRES_FIXED_VERSION (we
	// never constrain version, satisfying any REQUIRES_VERSION_ROLLING
	// client) or REQUIRES_EXTENDED_CHANNELS (we only open standard
	// channels) — both correctly stay unset (0).
	sv2ServerResponseFlags uint32 = 0
)

// JobTemplate is the minimal set of block-template data the Session needs
// to send work to miners.  It is populated by the engine's JobManager and
// pushed to all open sessions via Server.BroadcastJob().
type JobTemplate struct {
	JobID        uint32
	PrevHash     [32]byte
	Coinbase1    []byte
	Coinbase2    []byte
	MerkleBranch [][32]byte
	Version      uint32
	NBits        uint32
	NTime        uint32 // current time at template creation; miners may use ≥ this
	Height       uint32
	IsFutureJob  bool // true when we send before SetNewPrevHash (pre-staging)
}

// shareSubmitCallback is called by the session when a valid share arrives.
// The server registers this to forward block solutions to the node RPC.
type shareSubmitCallback func(job *JobTemplate, ch *Channel, share *MsgSubmitSharesStandardFields, result *ShareResult)

// Session represents one connected miner.
type Session struct {
	id    string   // log-friendly identifier (remote addr)
	conn  net.Conn // noise-encrypted connection
	codec *Codec

	mu       sync.RWMutex
	channels map[uint32]*Channel // channelID → Channel

	// Current block template (updated by BroadcastJob from the server).
	templateMu   sync.RWMutex
	lastTemplate *JobTemplate

	// Callback to the engine when a share / block solution is found.
	onShare      shareSubmitCallback
	onDisconnect func(workerName, remoteAddr string)

	// Solo-mode coinbase builder. Nil in pool mode — channels then use the
	// template's shared Coinbase1/Coinbase2 unmodified.
	coinbaseBuilder CoinbaseBuilderFunc

	// Variable difficulty config, passed through to every channel this
	// session opens. See newChannel's doc comment for the full picture —
	// this reuses pkg/stratum's VarDiff tracker directly, the same
	// algorithm and config V1 uses.
	vardiffCfg        *stratum.VarDiffConfig
	vardiffOnNewBlock bool
	startDiff         float64

	// ConnectionTimeoutSeconds is the read deadline per frame. If the miner
	// sends nothing for this long, the connection is dropped. Equivalent to
	// GSS's "Connection Timeout". Defaults to 600s if zero.
	connectionTimeoutSeconds int
	srv                      *Server

	// Structured logger (optional). When set, log output matches the engine's
	// format. When nil, falls back to log.Printf via s.logf.
	logger sv2Logger

	// Signals
	closeCh     chan struct{}
	once        sync.Once
	connectedAt time.Time
}

// logf logs at Info level via the structured logger if available, otherwise
// falls back to Go's stdlib log.Printf. All session log calls go through
// here so the output format is consistent with the rest of the engine.
func (s *Session) logf(format string, args ...interface{}) {
	if s.logger != nil {
		s.logger.Info(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// newSession wraps an already-handshaked conn, plus its transport-phase
// ciphers, in a Session.
func newSession(
	conn net.Conn,
	send, recv *sv2TransportCipher,
	onShare shareSubmitCallback,
	coinbaseBuilder CoinbaseBuilderFunc,
	vardiffCfg *stratum.VarDiffConfig,
	vardiffOnNewBlock bool,
	startDiff float64,
	logger sv2Logger,
	connectionTimeoutSeconds int,
	srv *Server,
	onDisconnect func(workerName, remoteAddr string),
) *Session {
	return &Session{
		id:                       conn.RemoteAddr().String(),
		connectedAt:              time.Now(),
		conn:                     conn,
		codec:                    NewCodec(conn, send, recv),
		channels:                 make(map[uint32]*Channel),
		onShare:                  onShare,
		coinbaseBuilder:          coinbaseBuilder,
		vardiffCfg:               vardiffCfg,
		vardiffOnNewBlock:        vardiffOnNewBlock,
		startDiff:                startDiff,
		connectionTimeoutSeconds: connectionTimeoutSeconds,
		srv:                      srv,
		logger:                   logger,
		closeCh:                  make(chan struct{}),
		onDisconnect:             onDisconnect,
	}
}

// Run is the main message-dispatch loop. Blocks until the session ends.
// Call in a goroutine: go session.Run()
func (s *Session) Run() {
	defer s.Close()
	s.logf("[sv2] session %s: connected", s.id)

	// Step 1: Wait for SetupConnection (must be first message).
	if err := s.handleSetupConnection(); err != nil {
		s.logf("[sv2] session %s: setup failed: %v", s.id, err)
		return
	}

	// Step 2: Main message loop.
	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		timeout := s.connectionTimeoutSeconds
		if timeout <= 0 {
			timeout = defaultReadDeadlineSeconds
		}
		if err := s.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Second)); err != nil {
			return
		}

		frame, err := s.codec.ReadFrame()
		if err != nil {
			s.logf("[sv2] session %s: read error: %v", s.id, err)
			return
		}

		if err := s.dispatch(frame); err != nil {
			s.logf("[sv2] session %s: dispatch error (msg=0x%02X): %v", s.id, frame.MsgType, err)
			// Non-fatal errors: log and continue. Fatal errors return from dispatch.
		}
	}
}

// handleSetupConnection reads and validates the first frame, which MUST be
// a SetupConnection message.
func (s *Session) handleSetupConnection() error {
	if err := s.conn.SetReadDeadline(time.Now().Add(handshakeTimeoutSeconds * time.Second)); err != nil {
		return err
	}
	defer s.conn.SetReadDeadline(time.Time{})

	frame, err := s.codec.ReadFrame()
	if err != nil {
		return fmt.Errorf("read SetupConnection: %w", err)
	}
	if frame.MsgType != MsgSetupConnection {
		return fmt.Errorf("expected SetupConnection (0x00), got 0x%02X", frame.MsgType)
	}

	sc, err := DecodeSetupConnection(frame.Payload)
	if err != nil {
		return fmt.Errorf("decode SetupConnection: %w", err)
	}

	// Validate: we only support the Mining Protocol.
	if sc.Protocol != ProtocolMining {
		payload, _ := EncodeSetupConnectionError(0, "unsupported-protocol")
		_ = s.codec.WriteFrame(ExtensionTypeMining, MsgSetupConnectionError, payload)
		return fmt.Errorf("unsupported protocol: %d", sc.Protocol)
	}

	// Validate version range.
	if sc.MaxVersion < sv2ProtocolVersion || sc.MinVersion > sv2ProtocolVersion {
		payload, _ := EncodeSetupConnectionError(sc.Flags, "protocol-version-mismatch")
		_ = s.codec.WriteFrame(ExtensionTypeMining, MsgSetupConnectionError, payload)
		return fmt.Errorf("version mismatch: client wants %d–%d, server is %d",
			sc.MinVersion, sc.MaxVersion, sv2ProtocolVersion)
	}

	// Reject REQUIRES_WORK_SELECTION — we don't implement SetCustomMiningJob.
	if sc.Flags&sv2FlagRequiresWorkSelection != 0 {
		unsupported := sc.Flags &^ sv2ClientFlagsAccepted
		payload, _ := EncodeSetupConnectionError(unsupported, "unsupported-feature-flags")
		_ = s.codec.WriteFrame(ExtensionTypeMining, MsgSetupConnectionError, payload)
		return fmt.Errorf("client requires unsupported feature flags: 0x%08X", unsupported)
	}

	s.logf("[sv2] session %s: SetupConnection OK — vendor=%q firmware=%q device=%q",
		s.id, sc.Vendor, sc.Firmware, sc.DeviceID)

	// Respond with success. SetupConnection.Success.flags is a SEPARATE
	// namespace from the client's request flags (sc.Flags) — see the
	// sv2ServerResponseFlags doc comment above. We don't echo sc.Flags back.
	payload := EncodeSetupConnectionSuccess(sv2ProtocolVersion, sv2ServerResponseFlags)
	return s.codec.WriteFrame(ExtensionTypeMining, MsgSetupConnectionSuccess, payload)
}

// dispatch routes an incoming frame to the appropriate handler.
func (s *Session) dispatch(frame *Frame) error {
	switch frame.MsgType {
	case MsgOpenStandardMiningChannel:
		return s.handleOpenChannel(frame.Payload)
	case MsgSubmitSharesStandard:
		return s.handleSubmitShares(frame.Payload)
	default:
		// Unknown/unhandled message types are silently ignored per the spec.
		s.logf("[sv2] session %s: unhandled msg type 0x%02X (%d bytes)", s.id, frame.MsgType, len(frame.Payload))
		return nil
	}
}

// handleOpenChannel processes an OpenStandardMiningChannel request.
func (s *Session) handleOpenChannel(payload []byte) error {
	req, err := DecodeOpenStandardMiningChannel(payload)
	if err != nil {
		return fmt.Errorf("decode OpenStandardMiningChannel: %w", err)
	}

	s.mu.Lock()
	if len(s.channels) >= maxChannelsPerSession {
		s.mu.Unlock()
		resp, _ := EncodeOpenStandardMiningChannelError(req.RequestID, "max-channels-exceeded")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgOpenStandardMiningChannelError, resp)
	}
	s.mu.Unlock()

	ch, err := newChannel(s.id, req.UserIdentity, globalExtranoncePool, s.vardiffCfg, s.vardiffOnNewBlock, s.startDiff)
	if err != nil {
		resp, _ := EncodeOpenStandardMiningChannelError(req.RequestID, "internal-error")
		_ = s.codec.WriteFrame(ExtensionTypeMining, MsgOpenStandardMiningChannelError, resp)
		return fmt.Errorf("newChannel: %w", err)
	}

	s.mu.Lock()
	s.channels[ch.ID()] = ch
	s.mu.Unlock()

	s.logf("[sv2] session %s: opened channel %d for worker %q (hashrate=%.2f GH/s)",
		s.id, ch.ID(), req.UserIdentity, float64(req.NominalHashrate)/1e9)

	// Solo mode: build this channel's own coinbase paying out to its
	// UserIdentity address. Errors are logged but non-fatal — the channel
	// falls back to the template's shared coinbase (which in solo mode is
	// the pool's fallback address) until a template update succeeds in
	// building this channel's coinbase.
	if s.coinbaseBuilder != nil {
		cb1, cb2, err := s.coinbaseBuilder(req.UserIdentity)
		if err != nil {
			s.logf("[sv2] session %s ch=%d: coinbase build failed for %q: %v",
				s.id, ch.ID(), req.UserIdentity, err)
		} else {
			ch.SetOwnCoinbase(cb1, cb2)
		}
	}

	// Send OpenStandardMiningChannel.Success.
	resp, err := EncodeOpenStandardMiningChannelSuccess(
		req.RequestID,
		ch.ID(),
		ch.PoolTarget(),
		ch.Extranonce1Bytes(),
		0, // groupChannelID = 0 (not using grouped channels)
	)
	if err != nil {
		return fmt.Errorf("encode OpenSuccess: %w", err)
	}
	if err := s.codec.WriteFrame(ExtensionTypeMining, MsgOpenStandardMiningChannelSuccess, resp); err != nil {
		return err
	}

	// Fetch the latest template directly from the server — no goroutine race.
	if s.srv == nil {
		s.logf("[sv2] session %s: srv is nil, cannot send initial job", s.id)
	} else if tmpl := s.srv.LatestTemplate(); tmpl == nil {
		s.logf("[sv2] session %s: no template available yet for initial job", s.id)
	} else {
			initTmpl := *tmpl // copy so we don't mutate the cached template
			initTmpl.IsFutureJob = false
			if err := s.sendPrevHashToChannel(ch, &initTmpl); err != nil {
				s.logf("[sv2] session %s: initial prevhash send failed: %v", s.id, err)
			} else
		if err := s.sendJobToChannel(ch, &initTmpl); err != nil {
			s.logf("[sv2] session %s: initial job send failed: %v", s.id, err)
		} else {
			s.logf("[sv2] session %s: initial job sent successfully", s.id)
			s.templateMu.Lock()
			s.lastTemplate = tmpl
			s.templateMu.Unlock()
		}
	}
	return nil
}

// handleSubmitShares processes a SubmitSharesStandard message.
func (s *Session) handleSubmitShares(payload []byte) error {
	share, err := DecodeSubmitSharesStandard(payload)
	if err != nil {
		return fmt.Errorf("decode SubmitSharesStandard: %w", err)
	}

	s.mu.RLock()
	ch, ok := s.channels[share.ChannelID]
	s.mu.RUnlock()
	if !ok {
		// Channel doesn't exist — send error and continue.
		resp, _ := EncodeSubmitSharesError(share.ChannelID, share.SequenceNum, "unknown-channel")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesError, resp)
	}

	// Validate job ID.
	if !ch.IsJobValid(share.JobID) {
		ch.RecordRejection()
		resp, _ := EncodeSubmitSharesError(share.ChannelID, share.SequenceNum, "stale-share")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesError, resp)
	}

	// Look up the template for this job.
	s.templateMu.RLock()
	tmpl := s.lastTemplate
	s.templateMu.RUnlock()

	if tmpl == nil || tmpl.JobID != share.JobID {
		ch.RecordRejection()
		resp, _ := EncodeSubmitSharesError(share.ChannelID, share.SequenceNum, "stale-share")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesError, resp)
	}

	// Validate against THIS channel's fixed merkle root — Standard Channels
	// have NO extranonce2 (it's fixed per-job, not per-share), and the root
	// MUST exactly match what was sent in NewMiningJob for this job_id, or
	// share validation would silently diverge from what the miner actually
	// hashed. See channelMerkleRoot's doc comment for the spec citation.
	merkleRoot := channelMerkleRoot(ch, tmpl)

	result, err := ValidateShare(
		share.Version,
		tmpl.PrevHash,
		merkleRoot,
		share.NTime,
		tmpl.NBits,
		share.Nonce,
		ch.PoolTarget(),
		tmpl.NBits,
	)
	if err != nil {
		return fmt.Errorf("ValidateShare: %w", err)
	}

	if !result.MeetsPool {
		ch.RecordRejection()
		s.logf("[sv2] session %s ch=%d: %s Share rejected (low-difficulty) hash=%s",
			s.id, share.ChannelID, ch.UserIdentity(), result.HashHex[:16])
		resp, _ := EncodeSubmitSharesError(share.ChannelID, share.SequenceNum, "low-difficulty")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesError, resp)
	}

	// Valid share.
	lastSeq, accepted, sumDiff, vdResult := ch.RecordShare(share.SequenceNum, result.Difficulty)

	// GSS-style vardiff diagnostic logging
	if diag := vdResult.DiagString(); diag != "" {
		if vdResult.Adjusted {
			reason := "shares_too_fast"
			if vdResult.ClampedDiff < vdResult.CurrentDiff {
				reason = "shares_too_slow"
			}
			s.logf("[sv2] session %s ch=%d: %s VARDIFF DIAG: %s",
				s.id, ch.ID(), ch.UserIdentity(), diag)
			s.logf("[sv2] session %s ch=%d: %s VARDIFF: Difficulty adjustment pending (will send with next job): %.0f -> %.0f (%s)",
				s.id, ch.ID(), ch.UserIdentity(), vdResult.CurrentDiff, vdResult.ClampedDiff, reason)
		} else if vdResult.Reason == "within_variance" {
			s.logf("[sv2] session %s ch=%d: %s VARDIFF: No adjustment - within_variance (difficulty stays at %.0f) | %s",
				s.id, ch.ID(), ch.UserIdentity(), vdResult.CurrentDiff, diag)
		}
	}
	s.logf("[sv2] session %s ch=%d: %s Share accepted (diff %.2f) hash=%s block=%v",
		s.id, share.ChannelID, ch.UserIdentity(), result.Difficulty, result.HashHex[:16], result.MeetsBlock)

	// Notify the engine (e.g., to submit block to node RPC).
	if s.onShare != nil {
		go s.onShare(tmpl, ch, share, result)
	}

	resp := EncodeSubmitSharesSuccess(share.ChannelID, lastSeq, accepted, sumDiff)
	return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesSuccess, resp)
}

// ──────────────────────────────────────────────────────────────────────────────
// Work Dispatch — called by the Server when a new block template arrives
// ──────────────────────────────────────────────────────────────────────────────

// UpdateTemplate stores the latest block template and broadcasts new work to
// all open channels on this session.
func (s *Session) UpdateTemplate(tmpl *JobTemplate) {
	s.templateMu.Lock()
	s.lastTemplate = tmpl
	s.templateMu.Unlock()

	s.mu.RLock()
	channels := make([]*Channel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	s.mu.RUnlock()

	for _, ch := range channels {
		if ch.IsClosed() {
			continue
		}

		// Solo mode: refresh this channel's own coinbase against the new
		// template BEFORE dispatching the job. The new block height/fees
		// invalidate any previously-built coinbase.
		if s.coinbaseBuilder != nil {
			cb1, cb2, err := s.coinbaseBuilder(ch.UserIdentity())
			if err != nil {
				s.logf("[sv2] session %s ch=%d: coinbase refresh failed: %v", s.id, ch.ID(), err)
				// Keep the previous coinbase rather than falling back to the
				// shared template coinbase — mining with a stale-but-valid
				// coinbase for one extra job is safer than silently paying
				// out to the wrong address.
			} else {
				ch.SetOwnCoinbase(cb1, cb2)
			}
		}

		// SetNewPrevHash first, then NewMiningJob.
		if err := s.sendPrevHashToChannel(ch, tmpl); err != nil {
			s.logf("[sv2] session %s ch=%d: sendPrevHash error: %v", s.id, ch.ID(), err)
			continue
		}
		if err := s.sendJobToChannel(ch, tmpl); err != nil {
			s.logf("[sv2] session %s ch=%d: sendJob error: %v", s.id, ch.ID(), err)
		}
	}
}

// channelMerkleRoot computes the final 32-byte merkle root for a channel's
// job, using that channel's own coinbase (solo mode override, or the
// template's shared pool-mode coinbase) and the template's merkle branch.
//
// IMPORTANT: Standard Channels have NO extranonce2 component in their
// coinbase — per sv2-spec 05-Mining-Protocol.md §5.3.15's merkle root
// algorithm: "extranonce, # null if standard channel". The coinbase is
// built from extranonce_prefix (the channel's Extranonce1) alone. An
// earlier version fabricated an extranonce2 from the share's sequence
// number, which both contradicts the spec and would have produced a
// DIFFERENT merkle root per share — meaning the job sent to the miner and
// the root used to validate its shares could never have matched.
func channelMerkleRoot(ch *Channel, tmpl *JobTemplate) [32]byte {
	coinb1, coinb2 := tmpl.Coinbase1, tmpl.Coinbase2
	if ownCb1, ownCb2, ok := ch.OwnCoinbase(); ok {
		coinb1, coinb2 = ownCb1, ownCb2
	}
	coinbaseTxHash := HashCoinbaseTx(coinb1, ch.Extranonce1Bytes(), nil, coinb2)
	return ComputeMerkleRoot(coinbaseTxHash, tmpl.MerkleBranch)
}

// sendPrevHashToChannel sends SetNewPrevHash to a single channel.
func (s *Session) sendPrevHashToChannel(ch *Channel, tmpl *JobTemplate) error {
	payload := EncodeSetNewPrevHash(
		ch.ID(),
		tmpl.JobID,
		tmpl.PrevHash,
		tmpl.NTime,
		tmpl.NBits,
	)
	return s.codec.WriteFrame(ExtensionTypeMining, MsgSetNewPrevHash, payload)
}

// sendJobToChannel sends NewMiningJob to a single channel and records the job ID.
//
// Standard Channel jobs carry a precomputed merkle_root (server-side), NOT
// raw coinbase/merkle-branch data for the client to assemble itself — that
// raw format is for Extended Channels only (NewExtendedMiningJob, a
// different message this package does not currently implement, since
// ForgeNX only opens Standard Channels with miners today).
// sendJobToChannel sends SetNewPrevHash (if applicable) and NewMiningJob to
// a single channel. Flushes any pending vardiff first — exactly mirroring
// V1's SendJob logic (pkg/stratum/session.go): send SetTarget with the new
// difficulty, reset the vardiff measurement window, THEN send the job, so
// the miner's first shares on this job are already at the correct target.
//
// When VarDiffOnNewBlock is true (the BCH default), the flush only happens
// on clean/active jobs (tmpl.IsFutureJob==false), not on pre-staged future
// jobs, to avoid flushing before the block boundary where it would be
// meaningless.
func (s *Session) sendJobToChannel(ch *Channel, tmpl *JobTemplate) error {
	ch.SetCurrentJob(tmpl.JobID)

	// Flush pending vardiff before sending the job, but only when appropriate:
	// - VarDiffOnNewBlock=false: always flush immediately (V1's "mid-block" mode)
	// - VarDiffOnNewBlock=true: only flush on active/clean jobs (new block boundary)
	if _, hasPending := ch.PendingDiff(); hasPending {
		shouldFlush := !ch.VarDiffOnNewBlock() || !tmpl.IsFutureJob
		if shouldFlush {
			targetBytes, newDiff, flushed := ch.FlushPendingDiff()
			if flushed {
				setTargetPayload, err := EncodeSetTarget(ch.ID(), targetBytes)
				if err != nil {
					return fmt.Errorf("encode SetTarget: %w", err)
				}
				if err := s.codec.WriteFrame(ExtensionTypeMining, MsgSetTarget, setTargetPayload); err != nil {
					return fmt.Errorf("send SetTarget: %w", err)
				}
				s.logf("[sv2] session %s ch=%d: %s VARDIFF: Difficulty updated to %.0f (sent with new job)", s.id, ch.ID(), ch.UserIdentity(), newDiff)
			}
		}
		// else: will flush on next clean job (VarDiffOnNewBlock=true, future job)
	}

	merkleRoot := channelMerkleRoot(ch, tmpl)

	// minNtimeSet=false: per spec, the FIRST NewMiningJob after a channel
	// opens (or any future-staged job sent before its matching
	// SetNewPrevHash) must have min_ntime unset. tmpl.IsFutureJob already
	// tracks this distinction from the engine side.
	payload := EncodeNewMiningJob(
		ch.ID(),
		tmpl.JobID,
		!tmpl.IsFutureJob, // minNtimeSet: true once this job is actually active
		tmpl.NTime,
		tmpl.Version,
		merkleRoot,
	)
	return s.codec.WriteFrame(ExtensionTypeMining, MsgNewMiningJob, payload)
}

// ──────────────────────────────────────────────────────────────────────────────
// Lifecycle
// ──────────────────────────────────────────────────────────────────────────────

// Close tears down the session and all its channels.
func (s *Session) Close() {
	s.once.Do(func() {
		close(s.closeCh)
		s.conn.Close()
		s.mu.Lock()
		for _, ch := range s.channels {
			ch.Close()
		}
		s.mu.Unlock()
		s.logf("[sv2] session %s: disconnected", s.id)
		if s.onDisconnect != nil {
			workerName := ""
			s.mu.RLock()
			for _, ch := range s.channels {
				workerName = ch.UserIdentity()
				break
			}
			s.mu.RUnlock()
			s.onDisconnect(workerName, s.id)
		}
	})
}

// ID returns a log-friendly identifier for this session.
func (s *Session) ID() string         { return s.id }
func (s *Session) RemoteAddr() string { return s.id }
func (s *Session) ConnectedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connectedAt
}
func (s *Session) Channels() []*Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Channel, 0, len(s.channels))
	for _, ch := range s.channels {
		out = append(out, ch)
	}
	return out
}

// ChannelCount returns the number of currently open channels.
func (s *Session) ChannelCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels)
}
