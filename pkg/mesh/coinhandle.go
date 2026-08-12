package mesh

import "github.com/ForgeNX/forgenx-engine/pkg/stratum"

// CoinHandle is the contract the mesh needs from each coin it can route to.
// It is satisfied by *engine.CoinRunner (via additive accessor methods). The
// mesh depends only on this interface and pkg/stratum types, never on the
// engine package, which keeps the dependency one-directional (engine imports
// mesh to wire coinrunners in; mesh never imports engine).
type CoinHandle interface {
	// Symbol is the coin's ticker (e.g. "DGB").
	Symbol() string

	// PayoutAddress is the coin's configured solo payout address. The mesh
	// registers this and pulls jobs paying to it, so all mesh hashrate on this
	// coin pays the coin's configured address (payout substitution).
	PayoutAddress() string

	// RegisterMeshAddress ensures the coin's JobManager builds coinbases for the
	// given address (the coin's payout). Idempotent; safe to call repeatedly.
	RegisterMeshAddress(addr string)

	// JobForAddress returns the coin's current job with the coinbase built for
	// addr. The returned *stratum.Job carries its own JobID (the coin's job-ID),
	// which the mesh stores in the job context for staleness/lookup. Returns nil
	// if no job is currently available (e.g. node syncing).
	JobForAddress(addr string) *stratum.Job

	// ValidateShare routes a share to the coin's real validator, presenting the
	// mesh's per-job session view. All coin-specific validation, block assembly,
	// and submission happen inside the coin's existing validator.
	ValidateShare(session stratum.ShareSession, share *stratum.ShareSubmission) error

	// Running reports whether the coin is currently able to serve work (node up,
	// stratum running). The scheduler skips coins that aren't running.
	Running() bool
}

// CoinRegistry gives the mesh access to coins by symbol. It is satisfied by an
// engine-side adapter that looks up coinrunners. Returns nil for unknown or
// unavailable coins.
type CoinRegistry interface {
	Coin(symbol string) CoinHandle
}
