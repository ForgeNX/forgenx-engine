package mesh

import (
	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// ShareRouter routes a share submitted by a mesh miner to the correct coin's
// validator. The miner submits against a mesh-scoped job-ID; the router looks up
// the MeshJobContext for that ID, restores the coin's own source job-ID on the
// share (so the coin's validator finds its job), presents the mesh session view
// (extranonce1 / address / difficulty for that job), and calls the coin's real
// ValidateShare — which performs all coin-specific validation, block assembly,
// and node submission.
type ShareRouter struct {
	registry CoinRegistry
	routes   *RouteMap
	logger   *logging.Logger
}

// NewShareRouter creates the router.
func NewShareRouter(registry CoinRegistry, routes *RouteMap) *ShareRouter {
	return &ShareRouter{
		registry: registry,
		routes:   routes,
		logger:   logging.New(logging.ModuleStratum),
	}
}

// Route dispatches a share to its originating coin's validator. It returns the
// validator's error (nil on accepted share) so the mesh listener can respond to
// the miner appropriately.
func (sr *ShareRouter) Route(ms *MinerSession, share *stratum.ShareSubmission) error {
	// The miner submitted against a mesh job-ID.
	meshJobID := share.JobID
	ctx := sr.routes.Get(meshJobID)
	if ctx == nil {
		// Unknown/expired job — treat as stale. Returning an error lets the
		// listener send the miner a rejection; the miner moves on to the next job.
		sr.logger.Debug("[mesh] share for unknown/expired job %s from %s", meshJobID, ms.Worker)
		return stratum.ErrJobNotFound
	}

	h := sr.registry.Coin(ctx.Coin)
	if h == nil {
		sr.logger.Debug("[mesh] share for unavailable coin %s from %s", ctx.Coin, ms.Worker)
		return stratum.ErrJobNotFound
	}

	// Restore the coin's own job-ID on the share so the coin's validator (which
	// looks up jobs by its own job-ID via jobMgr.GetJob) finds the job. We work on
	// a copy so we don't mutate the caller's share.
	coinShare := *share
	coinShare.JobID = ctx.SourceJobID

	// Present the mesh session view for this job and hand off to the coin's real
	// validator. All coin-specific logic (difficulty check, block detection, RTT,
	// SubmitBlock) happens inside ValidateShare.
	sess := NewMeshSession(ctx)
	return h.ValidateShare(sess, &coinShare)
}
