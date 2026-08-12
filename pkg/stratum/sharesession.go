package stratum

import "time"

// ShareSession is the minimal view of a miner session that share validation
// needs. The concrete *Session satisfies it directly. Nexus Mesh provides its
// own per-job implementation so shares can be routed to the correct coin's
// validator with the right extranonce1 / mining address / difficulty for that
// job, without reimplementing any coin-specific validation.
//
// It lives in pkg/stratum (rather than pkg/engine) so both the engine and the
// mesh can name the type without an import cycle: engine imports mesh to start
// it, so mesh must not import engine; both already import stratum.
type ShareSession interface {
	MiningAddress() string
	ExtraNonce1() string
	GetDifficulty() float64
	GetPrevDifficulty() (float64, time.Time)
}
