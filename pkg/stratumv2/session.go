package stratumv2

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
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

	// readDeadlineSeconds is the per-frame read deadline for established sessions.
	// Miners should send shares or keepalives; if silent for this long, drop.
	readDeadlineSeconds = 120

	// SV2 feature flags we advertise and accept.
	// Bit 0: REQUIRES_STANDARD_JOBS — we always send standard jobs.
	// Bit 1: ASYNC_JOB_MINING — not supported yet.
	// Bit 2: SOLUTION_ROLLBACK — not supported.
	// Bit 3: REQUIRES_VERSION_ROLLING — we support it (VersionRollingMask).
	sv2FlagsSupported uint32 = 0x00000009 // bits 0 + 3
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
	onShare shareSubmitCallback

	// Solo-mode coinbase builder. Nil in pool mode — channels then use the
	// template's shared Coinbase1/Coinbase2 unmodified.
	coinbaseBuilder CoinbaseBuilderFunc

	// Signals
	closeCh chan struct{}
	once    sync.Once
}

// newSession wraps an already-handshaked conn, plus its transport-phase
// ciphers, in a Session.
func newSession(conn net.Conn, send, recv *sv2TransportCipher, onShare shareSubmitCallback, coinbaseBuilder CoinbaseBuilderFunc) *Session {
	return &Session{
		id:              conn.RemoteAddr().String(),
		conn:            conn,
		codec:           NewCodec(conn, send, recv),
		channels:        make(map[uint32]*Channel),
		onShare:         onShare,
		coinbaseBuilder: coinbaseBuilder,
		closeCh:         make(chan struct{}),
	}
}

// Run is the main message-dispatch loop. Blocks until the session ends.
// Call in a goroutine: go session.Run()
func (s *Session) Run() {
	defer s.Close()
	log.Printf("[sv2] session %s: connected", s.id)

	// Step 1: Wait for SetupConnection (must be first message).
	if err := s.handleSetupConnection(); err != nil {
		log.Printf("[sv2] session %s: setup failed: %v", s.id, err)
		return
	}

	// Step 2: Main message loop.
	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		if err := s.conn.SetReadDeadline(time.Now().Add(readDeadlineSeconds * time.Second)); err != nil {
			return
		}

		frame, err := s.codec.ReadFrame()
		if err != nil {
			log.Printf("[sv2] session %s: read error: %v", s.id, err)
			return
		}

		if err := s.dispatch(frame); err != nil {
			log.Printf("[sv2] session %s: dispatch error (msg=0x%02X): %v", s.id, frame.MsgType, err)
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

	log.Printf("[sv2] session %s: SetupConnection OK — vendor=%q firmware=%q device=%q",
		s.id, sc.Vendor, sc.Firmware, sc.DeviceID)

	// Respond with success. Negotiate flags as intersection of both sides.
	negotiatedFlags := sc.Flags & sv2FlagsSupported
	payload := EncodeSetupConnectionSuccess(sv2ProtocolVersion, negotiatedFlags)
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
		log.Printf("[sv2] session %s: unhandled msg type 0x%02X (%d bytes)", s.id, frame.MsgType, len(frame.Payload))
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

	ch, err := newChannel(s.id, req.UserIdentity, globalExtranoncePool)
	if err != nil {
		resp, _ := EncodeOpenStandardMiningChannelError(req.RequestID, "internal-error")
		_ = s.codec.WriteFrame(ExtensionTypeMining, MsgOpenStandardMiningChannelError, resp)
		return fmt.Errorf("newChannel: %w", err)
	}

	s.mu.Lock()
	s.channels[ch.ID()] = ch
	s.mu.Unlock()

	log.Printf("[sv2] session %s: opened channel %d for worker %q (hashrate=%.2f GH/s)",
		s.id, ch.ID(), req.UserIdentity, float64(req.NominalHashrate)/1e9)

	// Solo mode: build this channel's own coinbase paying out to its
	// UserIdentity address. Errors are logged but non-fatal — the channel
	// falls back to the template's shared coinbase (which in solo mode is
	// the pool's fallback address) until a template update succeeds in
	// building this channel's coinbase.
	if s.coinbaseBuilder != nil {
		cb1, cb2, err := s.coinbaseBuilder(req.UserIdentity)
		if err != nil {
			log.Printf("[sv2] session %s ch=%d: coinbase build failed for %q: %v",
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
		Extranonce2Size,
		0, // groupChannelID = 0 (not using grouped channels)
	)
	if err != nil {
		return fmt.Errorf("encode OpenSuccess: %w", err)
	}
	if err := s.codec.WriteFrame(ExtensionTypeMining, MsgOpenStandardMiningChannelSuccess, resp); err != nil {
		return err
	}

	// If we already have a current template, send it immediately so the miner
	// can start working without waiting for the next ZMQ notification.
	s.templateMu.RLock()
	tmpl := s.lastTemplate
	s.templateMu.RUnlock()

	if tmpl != nil {
		if err := s.sendJobToChannel(ch, tmpl); err != nil {
			log.Printf("[sv2] session %s: initial job send failed: %v", s.id, err)
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

	// Build the coinbase transaction hash using the miner's extranonce2.
	// The extranonce2 is derived from the sequenceNum for a simple scheme.
	// In a full implementation the miner sends extranonce2 explicitly;
	// for SV2 Standard Channel it is implicit from the miner's counter.
	// We use a fixed 4-byte LE encoding of the sequence number as extranonce2.
	var en2 [4]byte
	en2[0] = byte(share.SequenceNum)
	en2[1] = byte(share.SequenceNum >> 8)
	en2[2] = byte(share.SequenceNum >> 16)
	en2[3] = byte(share.SequenceNum >> 24)

	// Solo mode: validate against THIS channel's own coinbase, not the
	// template's shared one — otherwise we'd validate (and potentially
	// submit) a block paying out to the wrong address.
	coinb1, coinb2 := tmpl.Coinbase1, tmpl.Coinbase2
	if ownCb1, ownCb2, ok := ch.OwnCoinbase(); ok {
		coinb1, coinb2 = ownCb1, ownCb2
	}

	coinbaseTxHash := HashCoinbaseTx(
		coinb1,
		ch.Extranonce1Bytes(),
		en2[:],
		coinb2,
	)
	merkleRoot := ComputeMerkleRoot(coinbaseTxHash, tmpl.MerkleBranch)

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
		log.Printf("[sv2] session %s ch=%d: share rejected (low difficulty) hash=%s",
			s.id, share.ChannelID, result.HashHex[:16])
		resp, _ := EncodeSubmitSharesError(share.ChannelID, share.SequenceNum, "low-difficulty")
		return s.codec.WriteFrame(ExtensionTypeMining, MsgSubmitSharesError, resp)
	}

	// Valid share.
	lastSeq, accepted, sumDiff := ch.RecordShare(share.SequenceNum, result.Difficulty)
	log.Printf("[sv2] session %s ch=%d: share accepted diff=%.2f hash=%s block=%v",
		s.id, share.ChannelID, result.Difficulty, result.HashHex[:16], result.MeetsBlock)

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
				log.Printf("[sv2] session %s ch=%d: coinbase refresh failed: %v", s.id, ch.ID(), err)
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
			log.Printf("[sv2] session %s ch=%d: sendPrevHash error: %v", s.id, ch.ID(), err)
			continue
		}
		if err := s.sendJobToChannel(ch, tmpl); err != nil {
			log.Printf("[sv2] session %s ch=%d: sendJob error: %v", s.id, ch.ID(), err)
		}
	}
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
func (s *Session) sendJobToChannel(ch *Channel, tmpl *JobTemplate) error {
	ch.SetCurrentJob(tmpl.JobID)

	coinb1, coinb2 := tmpl.Coinbase1, tmpl.Coinbase2
	if ownCb1, ownCb2, ok := ch.OwnCoinbase(); ok {
		coinb1, coinb2 = ownCb1, ownCb2
	}

	payload := EncodeNewMiningJob(
		ch.ID(),
		tmpl.JobID,
		tmpl.IsFutureJob,
		tmpl.Version,
		VersionRollingMask,
		tmpl.MerkleBranch,
		coinb1,
		coinb2,
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
		log.Printf("[sv2] session %s: disconnected", s.id)
	})
}

// ID returns a log-friendly identifier for this session.
func (s *Session) ID() string { return s.id }

// ChannelCount returns the number of currently open channels.
func (s *Session) ChannelCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels)
}
