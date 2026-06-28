package stratumv2

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
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
	// Extranonce1 is always 4 bytes in ForgeNX.
	// Extranonce2 size offered to miners is 4 bytes (total extranonce = 8 bytes).
	Extranonce1Size uint16 = 4
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

	// Current pool target (may change via SetTarget / vardiff).
	poolTargetBytes []byte  // 32-byte LE B0_32 representation
	poolDifficulty  float64 // human-readable difficulty (for logging / stats)

	// Job tracking
	currentJobID uint32 // the most recent job sent to this channel
	// staleJobIDs tracks the last N job IDs so we can detect stale shares.
	// SV2 spec says servers MUST accept shares for at least the last job.
	staleJobIDs [2]uint32

	// Share sequence tracking (for SubmitSharesSuccess cumulative acks).
	lastAckedSeq     uint32
	pendingSharesAcc uint64 // accumulated share difficulty since last ack

	// Statistics
	sharesAccepted uint64
	sharesRejected uint64
	totalDiff      float64

	// Closed flag.
	closed int32 // atomic; 1 = channel has been closed
}

// channelCounter is used to assign monotonically increasing channel IDs
// across all sessions on this server instance.
var channelCounter uint32

// newChannel allocates a new Channel with a unique ID and extranonce1.
func newChannel(sessionID string, userIdentity string, extranoncePool *extranoncePool) (*Channel, error) {
	id := atomic.AddUint32(&channelCounter, 1)

	en1, err := extranoncePool.Allocate()
	if err != nil {
		return nil, fmt.Errorf("sv2 newChannel: %w", err)
	}

	target := DifficultyToTarget(DefaultPoolDifficulty)
	targetBytes := TargetToBytes(target)

	return &Channel{
		id:              id,
		userIdentity:    userIdentity,
		sessionID:       sessionID,
		extranonce1:     en1,
		poolTargetBytes: targetBytes,
		poolDifficulty:  DefaultPoolDifficulty,
	}, nil
}

// ID returns the channel's server-assigned identifier.
func (c *Channel) ID() uint32 { return c.id }

// UserIdentity returns the miner-reported worker name.
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

// PoolTarget returns the current pool difficulty target as a 32-byte LE slice.
func (c *Channel) PoolTarget() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]byte, 32)
	copy(out, c.poolTargetBytes)
	return out
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

// RecordShare records a validated share.
// Returns (lastAckedSeq, acceptedCount, sumDiff) for building SubmitSharesSuccess.
func (c *Channel) RecordShare(seqNum uint32, diff float64) (uint32, uint32, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sharesAccepted++
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

	return seqNum, accepted, sumDiff
}

// RecordRejection records a rejected share.
func (c *Channel) RecordRejection() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sharesRejected++
}

// Stats returns a snapshot of the channel's cumulative statistics.
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
