// Package mesh implements Nexus Mesh: a single Stratum endpoint that a miner
// connects to once, whose hashrate is then proportionally time-sliced across
// multiple same-algorithm (SHA256d) coin chains by rotating warm templates.
//
// Design note on share routing: shares are validated by each coin's existing,
// proven ShareValidator (with all its coin-specific quirks — XEC RTT, block
// submission, etc.). The mesh does NOT reimplement validation. Instead, for
// every job it sends a miner it records a MeshJobContext, and when the share
// returns it presents that context to the originating coin's validator via a
// meshSession facade (implementing engine.ShareSession). This keeps all the
// hard, coin-specific validation logic in one proven place.
package mesh

import "time"

// MeshJobContext captures everything about a single job the mesh sent to a
// miner that isn't carried in the miner's share submission itself. It is stored
// keyed by the mesh job-ID so a returned share can be reconstructed and routed
// to the correct coin's validator.
//
// The fields mirror exactly what engine.ShareValidator.ValidateShare needs from
// a session: the mining address (which selects the per-address coinbase in solo
// mode), the extranonce1 (spliced into the coinbase), and the difficulty. The
// rest of what the validator needs (coinb1/coinb2, merkle branches, version,
// prevhash, target) lives in the coin's own JobManager keyed by SourceJobID, so
// we do not duplicate it here — we only need to route to the right coin and
// present the right session view.
type MeshJobContext struct {
	// Coin is the symbol of the originating coin (e.g. "DGB", "BCH"). Determines
	// which coinrunner's validator a returned share is routed to.
	Coin string

	// SourceJobID is the coin's own job-ID (as known to that coin's JobManager).
	// The mesh assigns miners a mesh-scoped job-ID, but the coin validator looks
	// up its job by the coin's job-ID, so we keep the mapping.
	SourceJobID string

	// MiningAddressVal is the coin's configured payout address that this job's
	// coinbase pays to. In solo mode the coin's JobManager keys coinbases by
	// address; the mesh always uses the coin's configured address (payout
	// substitution — the miner's own username address is ignored).
	MiningAddressVal string

	// ExtraNonce1Val is the extranonce1 assigned to this job for this miner.
	ExtraNonce1Val string

	// DifficultyVal is the per-route (per-miner, per-coin) share difficulty that
	// was in effect for this job. Nexus Mesh uses split vardiff: each miner gets
	// a difficulty tuned to its measured hashrate.
	DifficultyVal float64

	// CreatedAt is when the job was issued, used by the routemap to expire old
	// contexts so the map does not grow unbounded.
	CreatedAt time.Time
}

// meshSession adapts a MeshJobContext to the engine.ShareSession interface so a
// returned share can be handed to the originating coin's ShareValidator exactly
// as if it had come from a real per-coin session. It implements the four methods
// the validator reads: MiningAddress, ExtraNonce1, GetDifficulty,
// GetPrevDifficulty.
type meshSession struct {
	ctx *MeshJobContext
}

// NewMeshSession returns a ShareSession-compatible view of a stored job context.
func NewMeshSession(ctx *MeshJobContext) *meshSession {
	return &meshSession{ctx: ctx}
}

func (m *meshSession) MiningAddress() string  { return m.ctx.MiningAddressVal }
func (m *meshSession) ExtraNonce1() string    { return m.ctx.ExtraNonce1Val }
func (m *meshSession) GetDifficulty() float64 { return m.ctx.DifficultyVal }

// GetPrevDifficulty returns no previous difficulty for mesh jobs. Each mesh job
// carries a single fixed difficulty, so the low-difficulty grace window (which
// exists to cover vardiff transitions within one coin session) does not apply.
// Returning a zero value disables that grace path for mesh shares, which is the
// correct behavior: a mesh share is judged against exactly the difficulty its
// job was issued at.
func (m *meshSession) GetPrevDifficulty() (float64, time.Time) {
	return 0, time.Time{}
}
