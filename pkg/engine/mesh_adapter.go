package engine

import (
	"github.com/ForgeNX/forgenx-engine/pkg/mesh"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// This file is the additive seam that lets the engine's coinrunners satisfy the
// mesh.CoinHandle contract, and lets the Engine act as a mesh.CoinRegistry. It
// adds only new methods — no existing engine behavior changes. Nexus Mesh
// consumes these; direct-connect coin mining is untouched.

// --- CoinRunner satisfies mesh.CoinHandle ---

// Symbol returns the coin's ticker.
func (r *CoinRunner) Symbol() string { return r.symbol }

// PayoutAddress returns the coin's configured payout address. Nexus Mesh pays all
// mesh-mined blocks on this coin to this address (payout substitution — the
// miner's supplied address is ignored, since it can't be valid across chains).
//
// MONEY-CRITICAL: this is where mesh block rewards land. The address comes from
// cfg.Mining.Address. Before trusting the mesh with real mining, each meshed
// coin's address MUST be validated on-chain (coin.ValidateAddress) — a valid-but-
// wrong address would silently misroute rewards. The scheduler skips coins whose
// PayoutAddress is empty (fail-safe), but does not yet validate correctness.
func (r *CoinRunner) PayoutAddress() string { return r.miningAddress }

// RegisterMeshAddress ensures the job manager builds coinbases for addr.
func (r *CoinRunner) RegisterMeshAddress(addr string) { r.jobMgr.RegisterAddress(addr) }

// JobForAddress returns the coin's current job paying to addr (carries the coin's
// own job-ID), or nil if none is available.
func (r *CoinRunner) JobForAddress(addr string) *stratum.Job {
	return r.jobMgr.GetJobForAddress(addr)
}

// ValidateShare routes a share to the coin's real validator, which performs all
// coin-specific validation, block assembly, and node submission.
func (r *CoinRunner) ValidateShare(session stratum.ShareSession, share *stratum.ShareSubmission) error {
	return r.validator.ValidateShare(session, share)
}

// Running reports whether the coin can currently serve work.
func (r *CoinRunner) Running() bool { return r.StratumRunning() }

// --- Engine satisfies mesh.CoinRegistry ---

// Coin returns a mesh.CoinHandle for the given symbol, or nil if there is no such
// runner. The explicit nil return (rather than returning a nil *CoinRunner) is
// required so the mesh's `h == nil` checks work: a typed-nil pointer wrapped in an
// interface is NOT == nil.
func (e *Engine) Coin(symbol string) mesh.CoinHandle {
	e.runnersMu.RLock()
	r, ok := e.runners[symbol]
	e.runnersMu.RUnlock()
	if !ok || r == nil {
		return nil
	}
	return r
}

// compile-time assertions that the contracts are satisfied.
var (
	_ mesh.CoinHandle   = (*CoinRunner)(nil)
	_ mesh.CoinRegistry = (*Engine)(nil)
)
