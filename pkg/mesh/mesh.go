package mesh

import (
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// Mesh is the assembled Nexus Mesh: a listener, a scheduler, and a share router
// wired together. The engine creates one Mesh, gives it a CoinRegistry (adapter
// over the coinrunners), and starts it on a dedicated Stratum port. Miners
// connect once to that port and have their hashrate proportionally time-sliced
// across their allocated SHA256d coins.
type Mesh struct {
	listener  *Listener
	scheduler *Scheduler
	router    *ShareRouter
	routes    *RouteMap
	logger    *logging.Logger

	// defaultAlloc is applied to each miner on authorize until a custom split is
	// set via SetAllocation. Nil/empty means miners start with no work until the
	// UI assigns an allocation.
	defaultAlloc []CoinWeight
}

// Options configures a Mesh.
type Options struct {
	Host           string
	Port           int
	ExtraNonceSize int
	DefaultDiff    float64
	// VarDiff bounds/targets for per-miner difficulty adjustment. Zero values take
	// sensible defaults in New(). These will eventually be driven by the Nexus UI.
	MinDiff      float64
	MaxDiff      float64
	TargetTime   float64
	RetargetTime float64
	VariancePct  float64
	// RotateInterval is the coin-slice duration; clamped to <=30s for watchdog
	// safety. Zero uses the default (20s).
	RotateInterval time.Duration
	// RouteTTL is how long job contexts are retained for share lookup. Zero uses
	// the default (5m).
	RouteTTL time.Duration
}

// New assembles a Mesh from the given registry and options, wiring the listener
// hooks to the scheduler and router. It does not start listening until Start.
func New(registry CoinRegistry, opts Options) *Mesh {
	if opts.ExtraNonceSize == 0 {
		opts.ExtraNonceSize = 4
	}
	if opts.DefaultDiff <= 0 {
		opts.DefaultDiff = 4096
	}
	if opts.MinDiff <= 0 {
		opts.MinDiff = 4096
	}
	if opts.MaxDiff <= 0 {
		opts.MaxDiff = 262144
	}
	if opts.TargetTime <= 0 {
		opts.TargetTime = 8
	}
	if opts.RetargetTime <= 0 {
		opts.RetargetTime = 90
	}
	if opts.VariancePct <= 0 {
		opts.VariancePct = 30
	}

	routes := NewRouteMap(opts.RouteTTL)
	scheduler := NewScheduler(registry, routes, opts.RotateInterval, opts.DefaultDiff)
	router := NewShareRouter(registry, routes)

	listener := NewListener(Config{
		Host:           opts.Host,
		Port:           opts.Port,
		ExtraNonceSize: opts.ExtraNonceSize,
		DefaultDiff:    opts.DefaultDiff,
		MinDiff:        opts.MinDiff,
		MaxDiff:        opts.MaxDiff,
		TargetTime:     opts.TargetTime,
		RetargetTime:   opts.RetargetTime,
		VariancePct:    opts.VariancePct,
	})

	m := &Mesh{
		listener:  listener,
		scheduler: scheduler,
		router:    router,
		routes:    routes,
		logger:    logging.New(logging.ModuleStratum),
	}

	// Wire listener hooks:
	//  - a newly-authorized miner starts rotation
	//  - a departed miner stops rotation
	//  - each share is routed to its originating coin's validator
	listener.SetOnAuthorized(func(ms *MinerSession) {
		if len(m.defaultAlloc) > 0 {
			scheduler.SetAllocation(ms, NewAllocation(m.defaultAlloc))
		}
		scheduler.StartMiner(ms)
	})
	listener.SetOnRemoved(func(ms *MinerSession) {
		scheduler.StopMiner(ms)
	})
	listener.SetOnShare(func(ms *MinerSession, share *stratum.ShareSubmission) error {
		return router.Route(ms, share)
	})

	return m
}

// Start begins accepting miners.
func (m *Mesh) Start() error {
	m.logger.Info("[mesh] Nexus Mesh starting")
	return m.listener.Start()
}

// Stop shuts the mesh down.
func (m *Mesh) Stop() {
	m.listener.Stop()
	m.logger.Info("[mesh] Nexus Mesh stopped")
}

// SetDefaultAllocation sets the allocation applied to each miner on connect until
// a custom split is assigned. Safe to call before Start.
func (m *Mesh) SetDefaultAllocation(weights []CoinWeight) {
	m.defaultAlloc = weights
}

// SetAllocation sets or updates a miner's coin split. The UI calls this when the
// user adjusts the split sliders. ms is identified by matching against live
// sessions; callers typically obtain it from Sessions().
func (m *Mesh) SetAllocation(ms *MinerSession, alloc *Allocation) {
	m.scheduler.SetAllocation(ms, alloc)
}

// Sessions returns a snapshot of live mesh miner sessions (for the UI board).
func (m *Mesh) Sessions() []*MinerSession {
	return m.listener.Sessions()
}
