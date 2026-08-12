package mesh

import (
	"sync"
	"time"
)

// RouteMap is the thread-safe store of MeshJobContext keyed by the mesh job-ID
// the miner was given. When a share comes back carrying that job-ID, the router
// looks the context up here to learn which coin the share belongs to and how to
// present it to that coin's validator.
//
// Contexts are pruned after ttl so the map does not grow without bound: a miner
// only ever submits against reasonably recent jobs, so old contexts are safe to
// drop. Pruning is lazy (on write) plus an optional background sweeper.
type RouteMap struct {
	mu  sync.RWMutex
	m   map[string]*MeshJobContext
	ttl time.Duration
}

// NewRouteMap creates a RouteMap. ttl controls how long a job context is
// retained after creation; jobs older than ttl are pruned. A ttl of a few
// minutes comfortably covers any in-flight share while bounding memory.
func NewRouteMap(ttl time.Duration) *RouteMap {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &RouteMap{
		m:   make(map[string]*MeshJobContext),
		ttl: ttl,
	}
}

// Put stores a job context under its mesh job-ID and opportunistically prunes
// expired entries.
func (r *RouteMap) Put(meshJobID string, ctx *MeshJobContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[meshJobID] = ctx
	r.pruneLocked(time.Now())
}

// Get returns the context for a mesh job-ID, or nil if unknown or expired.
func (r *RouteMap) Get(meshJobID string) *MeshJobContext {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ctx, ok := r.m[meshJobID]
	if !ok {
		return nil
	}
	return ctx
}

// Delete removes a context (e.g. after a share for it is processed, if desired).
func (r *RouteMap) Delete(meshJobID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, meshJobID)
}

// Len returns the current number of stored contexts (useful for metrics/tests).
func (r *RouteMap) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.m)
}

// pruneLocked drops entries older than ttl. Caller must hold r.mu.
func (r *RouteMap) pruneLocked(now time.Time) {
	for id, ctx := range r.m {
		if now.Sub(ctx.CreatedAt) > r.ttl {
			delete(r.m, id)
		}
	}
}

// Prune removes expired entries. Safe to call from a background sweeper.
func (r *RouteMap) Prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now())
}
