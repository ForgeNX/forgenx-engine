package stratumv2

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"sync/atomic"

	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// ──────────────────────────────────────────────────────────────────────────────
// Standard Mining Channel
//
// A single TCP connection (session) can open multiple Standard Mining Channels.
// Each channel gets:
//   • A server-assigned channelID (unique within the session)
//   • An extranonce1 allocation (4 bytes from the server's pool)
//   • A pool-difficulty target (may differ per-channel based on hashrate)
//   • Its own sequence number tracking for share acknowledgement
//
// The channel is the unit of work dispatch and share accounting.
// ──────────────────────────────────────────────────────────────────────────────

const (
	// Extranonce1Size is the size of the server-assigned extranonce_prefix
	// sent in OpenStandardMiningChannel.Success.
	Extranonce1Size uint16 = 4

	// Extranonce2Size is currently unused: Standard Channels have NO
	// extranonce2 component at all (see channelMerkleRoot's doc comment in
	// session.go for the spec citation) — the channel's full extranonce IS
	// extranonce_prefix. This constant is kept for documentation purposes
	// and in case Extended Channel support is added later (where extranonce2
	// genuinely does exist, miner-rolled, per share).
	Extranonce2Size uint16 = 4

	// DefaultPoolDifficulty is the starting pool difficulty assigned to new channels.
	// This is intentionally low so miners start finding shares immediately.
	// Vardiff (not yet implemented) will raise it based on hashrate.
	DefaultPoolDifficulty float64 = 512.0

	// VersionRollingMask is the set of version bits miners are allowed to roll.
	// BIP 320: bits 13-0 of the version field (0x1FFFE000 is the mask).
	// We allow a conservative subset here.
	VersionRollingMask uint32 = 0x1FFFE000
)

// Channel represents one Standard Mining Channel within a session.
type Channel struct {
	mu sync.RWMutex

	// Identity
	id           uint32 // server-assigned channel ID
	userIdentity string // miner-reported worker name / address
	sessionID    string // parent session identifier (for logging)

	// Extranonce allocation
	extranonce1 [4]byte // assigned at open time, immutable

	// isExtended distinguishes Extended Mining Channels (client builds its
	// own coinbase, rolls its own extranonce2) from Standard Mining
	// Channels (server computes everything, fixed extranonce1 only).
	// extranonce2Size is meaningless when isExtended is false.
	isExtended      bool
	extranonce2Size uint16

	// Current pool target (may change via SetTarget / vardiff).
	poolTargetBytes []byte  // 32-byte LE B0_32 representation
	poolDifficulty  float64 // human-readable difficulty (for logging / stats)

	// Variable difficulty. Reuses pkg/stratum's VarDiff tracker directly —
	// same algorithm V1 uses, fed from the same cfg.VarDiff config, so V1
	// and SV2 channels mining the same coin retarget identically. nil if
	// vardiff is disabled for this coin (cfg.VarDiff.Enabled == false).
	//
	// Mirrors V1's pendingDiff pattern exactly (see pkg/stratum/session.go):
	// a retarget does NOT immediately push SetTarget mid-job — it's queued
	// here and flushed on the next job dispatch (or immediately if
	// vardiffOnNewBlock is false), avoiding a difficulty change mid-search
	// that would desync the miner's nonce space.
	vardiff           *stratum.VarDiff
	pendingDiff       float64   // 0 = none pending
	prevDiff          float64   // previous difficulty before last change (for grace period)
	diffChangedAt     time.Time // when difficulty last changed
	vardiffOnNewBlock bool      // mirrors ServerConfig.VarDiffOnNewBlock from V1

	// Job tracking
	currentJobID uint32 // the most recent job sent to this channel
	// staleJobIDs tracks the last N job IDs so we can detect stale shares.
	// SV2 spec says servers MUST accept shares for at least the last job.
	staleJobIDs [2]uint32

	// Solo-mode per-channel coinbase override. When set (non-nil), the
	// channel's own coinbase1/coinbase2 are used instead of the shared
	// JobTemplate.Coinbase1/2 — this is how each SV2 worker's payout
	// address gets baked into its own block solution, mirroring V1's
	// AddressCoinb2s map but resolved per-channel instead of precomputed
	// for all known addresses. Pool mode never sets this; nil means
	// "use the template's shared coinbase."
	coinbaseMu   sync.RWMutex
	ownCoinbase1 []byte
	ownCoinbase2 []byte

	// Share sequence tracking (for SubmitSharesSuccess cumulative acks).
	lastAckedSeq     uint32
	pendingSharesAcc uint64 // accumulated share difficulty since last ack

	// Statistics
	sharesAccepted  uint64
	bestDifficulty  float64
	sharesRejected uint64
	totalDiff      float64

	// Closed flag.
	closed int32 // atomic; 1 = channel has been closed
}

// channelCounter is used to assign monotonically increasing channel IDs
// across all sessions on this server instance.
var channelCounter uint32

// newChannel allocates a new Channel with a unique ID and extranonce1.
//
// vardiffCfg may be nil (vardiff disabled for this coin); startDiff is the
// channel's initial pool difficulty, normally cfg.Stratum.Difficulty —
// the same value V1 uses as its starting point before any retargeting.
func newChannel(
	sessionID string,
	userIdentity string,
	extranoncePool *extranoncePool,
	vardiffCfg *stratum.VarDiffConfig,
	vardiffOnNewBlock bool,
	startDiff float64,
) (*Channel, error) {
	id := atomic.AddUint32(&channelCounter, 1)

	en1, err := extranoncePool.Allocate()
	if err != nil {
		return nil, fmt.Errorf("sv2 newChannel: %w", err)
	}

	if startDiff <= 0 {
		startDiff = DefaultPoolDifficulty
	}

	target := DifficultyToTarget(startDiff)
	targetBytes := TargetToBytes(target)

	ch := &Channel{
		id:                id,
		userIdentity:      userIdentity,
		sessionID:         sessionID,
		extranonce1:       en1,
		poolTargetBytes:   targetBytes,
		poolDifficulty:    startDiff,
		vardiffOnNewBlock: vardiffOnNewBlock,
	}

	if vardiffCfg != nil {
		ch.vardiff = stratum.NewVarDiff(*vardiffCfg, startDiff)
	}

	return ch, nil
}

// maxExtranonce2Size caps how much extranonce2 space we grant an extended
// channel, regardless of what it requests. Kept small and fixed for now
// (minimal/option-1 implementation — one NerdQAxe++, not a multi-miner
// extended-channel pool with per-miner nonce-space partitioning).
const maxExtranonce2Size uint16 = 6

// newExtendedChannel allocates a new extended-channel Channel. Mirrors
// newChannel, but records isExtended + the granted extranonce2Size (clamped
// to maxExtranonce2Size, and to at least 1 byte so the miner has SOME
// rolling room even if it asked for 0).
func newExtendedChannel(
	sessionID string,
	userIdentity string,
	extranoncePool *extranoncePool,
	vardiffCfg *stratum.VarDiffConfig,
	vardiffOnNewBlock bool,
	startDiff float64,
	requestedExtranonceSize uint16,
) (*Channel, error) {
	ch, err := newChannel(sessionID, userIdentity, extranoncePool, vardiffCfg, vardiffOnNewBlock, startDiff)
	if err != nil {
		return nil, err
	}
	ch.isExtended = true
	granted := requestedExtranonceSize
	if granted == 0 || granted > maxExtranonce2Size {
		granted = maxExtranonce2Size
	}
	ch.extranonce2Size = granted
	return ch, nil
}

// IsExtended reports whether this is an Extended Mining Channel.
func (c *Channel) IsExtended() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isExtended
}

// ExtranonceSize returns the extranonce2 space (in bytes) granted to this
// extended channel. Meaningless for standard channels.
func (c *Channel) ExtranonceSize() uint16 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.extranonce2Size
}

// ID returns the channel's server-assigned identifier.
func (c *Channel) ID() uint32 { return c.id }

// UserIdentity returns the miner-reported worker name.
func (c *Channel) Difficulty() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.poolDifficulty
}
func (c *Channel) UserIdentity() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userIdentity
}

// Extranonce1 returns the 4-byte extranonce1 assigned to this channel.
func (c *Channel) Extranonce1() [4]byte { return c.extranonce1 }

// Extranonce1Bytes returns extranonce1 as a byte slice (convenience).
func (c *Channel) Extranonce1Bytes() []byte {
	b := c.extranonce1
	return b[:]
}

// SetOwnCoinbase stores a per-channel coinbase override (solo mode).
// Pass nil, nil to clear the override and fall back to the template's
// shared coinbase (pool mode behavior).
func (c *Channel) SetOwnCoinbase(coinbase1, coinbase2 []byte) {
	c.coinbaseMu.Lock()
	defer c.coinbaseMu.Unlock()
	c.ownCoinbase1 = coinbase1
	c.ownCoinbase2 = coinbase2
}

// OwnCoinbase returns the channel's coinbase override, and whether one is
// set. When ok is false, callers should fall back to the template's shared
// Coinbase1/Coinbase2 fields (pool mode).
func (c *Channel) OwnCoinbase() (coinbase1, coinbase2 []byte, ok bool) {
	c.coinbaseMu.RLock()
	defer c.coinbaseMu.RUnlock()
	if c.ownCoinbase1 == nil && c.ownCoinbase2 == nil {
		return nil, nil, false
	}
	return c.ownCoinbase1, c.ownCoinbase2, true
}

// PoolTarget returns the current pool difficulty target as a 32-byte LE slice.
func (c *Channel) PoolTarget() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]byte, 32)
	copy(out, c.poolTargetBytes)
	return out
}

// PoolDifficulty returns the current pool difficulty as a float.
func (c *Channel) PoolDifficulty() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.poolDifficulty
}

// GetPrevDifficulty returns the previous difficulty and when it last changed.
// Used to implement the low-diff grace period in share validation.
func (c *Channel) GetPrevDifficulty() (float64, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.prevDiff, c.diffChangedAt
}

// SetPoolDifficulty updates the channel's pool target.
// Returns the new target bytes so the caller can send SetTarget to the miner.
func (c *Channel) SetPoolDifficulty(diff float64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := DifficultyToTarget(diff)
	c.poolTargetBytes = TargetToBytes(target)
	c.poolDifficulty = diff
	out := make([]byte, 32)
	copy(out, c.poolTargetBytes)
	return out
}

// SetCurrentJob records the most recent job ID sent to this channel.
// The previous job ID is pushed into staleJobIDs[0] so we can accept
// shares on it for one job cycle.
func (c *Channel) SetCurrentJob(jobID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.staleJobIDs[1] = c.staleJobIDs[0]
	c.staleJobIDs[0] = c.currentJobID
	c.currentJobID = jobID
}

// IsJobValid returns true if jobID is the current job or one of the stale
// (recently-superseded) jobs we still accept.
func (c *Channel) IsJobValid(jobID uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return jobID == c.currentJobID ||
		jobID == c.staleJobIDs[0] ||
		jobID == c.staleJobIDs[1]
}

// RecordShare records a validated share, and — if vardiff is enabled —
// checks whether a retarget is due. Mirrors V1's exact behavior
// (pkg/stratum/session.go's handleSubmit): if a difficulty change is
// already pending delivery, the retarget check is SKIPPED for this share,
// since shares at the old difficulty would otherwise feed the vardiff
// window with increasingly-wrong assumptions while waiting for the new
// target to actually reach the miner.
//
// Returns (lastAckedSeq, acceptedCount, sumDiff) for building
// SubmitSharesSuccess — unchanged from before vardiff was added.
func (c *Channel) RecordShare(seqNum uint32, diff float64) (uint32, uint32, uint64, stratum.VarDiffResult) {
	c.mu.Lock()
	c.sharesAccepted++
	if diff > c.bestDifficulty {
		c.bestDifficulty = diff
	}
	c.totalDiff += diff

	// Cumulative ack: acknowledge everything up to seqNum.
	accepted := seqNum - c.lastAckedSeq
	c.lastAckedSeq = seqNum

	// SV2 SubmitSharesSuccess.new_shares_sum is the integer difficulty sum
	// scaled to the share difficulty units. We use uint64 truncation here.
	diffInt := uint64(diff)
	c.pendingSharesAcc += diffInt
	sumDiff := c.pendingSharesAcc
	c.pendingSharesAcc = 0

	hasPending := c.pendingDiff > 0
	vd := c.vardiff
	c.mu.Unlock()

	var vdResult stratum.VarDiffResult
	if vd != nil && !hasPending {
		vdResult = vd.RecordShare()
		if vdResult.Adjusted {
			c.mu.Lock()
			c.pendingDiff = vdResult.ClampedDiff
			c.mu.Unlock()
		}
	}

	return seqNum, accepted, sumDiff, vdResult
}

// PendingDiff returns the queued-but-not-yet-delivered vardiff target, and
// whether one exists. Callers (session.go's job dispatch path) should call
// FlushPendingDiff to actually apply it and clear the pending state.
func (c *Channel) PendingDiff() (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pendingDiff, c.pendingDiff > 0
}

// VarDiffOnNewBlock reports whether this channel's pending diff should only
// flush on clean (new-block) jobs, mirroring V1's ServerConfig.VarDiffOnNewBlock.
func (c *Channel) VarDiffOnNewBlock() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vardiffOnNewBlock
}

// FlushPendingDiff applies the channel's pending vardiff target (if any),
// resets the vardiff measurement window, and returns the new target bytes
// to send via SetTarget. Returns ok=false if nothing was pending.
func (c *Channel) FlushPendingDiff() (targetBytes []byte, newDiff float64, ok bool) {
	c.mu.Lock()
	if c.pendingDiff <= 0 {
		c.mu.Unlock()
		return nil, 0, false
	}
	newDiff = c.pendingDiff
	c.pendingDiff = 0
	c.prevDiff = c.poolDifficulty
	c.diffChangedAt = time.Now()
	target := DifficultyToTarget(newDiff)
	c.poolTargetBytes = TargetToBytes(target)
	c.poolDifficulty = newDiff
	out := make([]byte, 32)
	copy(out, c.poolTargetBytes)
	vd := c.vardiff
	c.mu.Unlock()

	if vd != nil {
		vd.ResetWindow(newDiff)
	}

	return out, newDiff, true
}

// RecordRejection records a rejected share.
func (c *Channel) RecordRejection() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sharesRejected++
}

// Stats returns a snapshot of the channel's cumulative statistics.
func (c *Channel) BestDifficulty() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bestDifficulty
}

func (c *Channel) Stats() (accepted, rejected uint64, totalDiff float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sharesAccepted, c.sharesRejected, c.totalDiff
}

// IsClosed returns true if this channel has been shut down.
func (c *Channel) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// Close marks the channel as closed (does not close the underlying connection).
func (c *Channel) Close() {
	atomic.StoreInt32(&c.closed, 1)
}

// ──────────────────────────────────────────────────────────────────────────────
// Extranonce Pool
//
// The server allocates 4-byte extranonce1 values from a 32-bit counter.
// Each channel gets a unique value; when the session closes, values are
// returned for re-use (simple incrementing counter, no recycling needed
// in practice for a solo-mining server with few concurrent connections).
// ──────────────────────────────────────────────────────────────────────────────

type extranoncePool struct {
	mu      sync.Mutex
	counter uint32
}

var globalExtranoncePool = &extranoncePool{}

// Allocate returns the next available extranonce1 as a [4]byte.
func (p *extranoncePool) Allocate() ([4]byte, error) {
	p.mu.Lock()
	v := p.counter
	p.counter++
	p.mu.Unlock()

	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return b, nil
}
