package mesh

import "sync"

// CoinWeight is one coin's share of a miner's hashrate within an allocation.
type CoinWeight struct {
	// Coin is the coin symbol (e.g. "DGB").
	Coin string
	// Percent is the target share of this miner's hashrate for this coin, 0..100.
	// The scheduler uses these weights to decide how often to rotate a job from
	// this coin to the miner. Percentages across an allocation are expected to
	// sum to 100 (the scheduler normalizes defensively if they do not).
	Percent float64
	// Pinned marks a coin the user has fixed in the allocation (the UI's PIN);
	// pinned coins are not auto-adjusted by any future auto-balancing logic.
	Pinned bool
}

// Allocation is a miner's full hashrate split across coins. It is the data the
// Nexus Mesh UI's split sliders produce and the scheduler consumes. "Only" mode
// (the UI's ONLY) is represented as a single 100% weight.
type Allocation struct {
	mu      sync.RWMutex
	weights []CoinWeight
}

// NewAllocation creates an allocation from the given weights (copied).
func NewAllocation(weights []CoinWeight) *Allocation {
	a := &Allocation{}
	a.Set(weights)
	return a
}

// Set replaces the allocation's weights (copied so the caller can't mutate them
// underneath the scheduler).
func (a *Allocation) Set(weights []CoinWeight) {
	cp := make([]CoinWeight, len(weights))
	copy(cp, weights)
	a.mu.Lock()
	a.weights = cp
	a.mu.Unlock()
}

// Weights returns a snapshot copy of the current weights.
func (a *Allocation) Weights() []CoinWeight {
	a.mu.RLock()
	defer a.mu.RUnlock()
	cp := make([]CoinWeight, len(a.weights))
	copy(cp, a.weights)
	return cp
}

// Coins returns just the coin symbols in the allocation.
func (a *Allocation) Coins() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, len(a.weights))
	for i, w := range a.weights {
		out[i] = w.Coin
	}
	return out
}

// TotalPercent sums the allocation's percentages (should be ~100).
func (a *Allocation) TotalPercent() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var t float64
	for _, w := range a.weights {
		t += w.Percent
	}
	return t
}

// Empty reports whether the allocation has no coins.
func (a *Allocation) Empty() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.weights) == 0
}
