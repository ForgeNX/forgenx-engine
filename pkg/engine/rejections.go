package engine

import (
	"strings"
	"sync"
	"time"
)

// Rejections keeps the most recent rejected shares per worker, so the reason a
// share was refused can be seen where it matters rather than only in the log.
// Kept in memory: it is a short window for diagnosis, not a record to preserve.
const rejectionsPerWorker = 10

// Rejection is one refused share.
type Rejection struct {
	At       time.Time `json:"at"`
	Coin     string    `json:"coin"`
	Worker   string    `json:"worker"`
	Reason   string    `json:"reason"` // "low difficulty", "stale", "unknown job", "no coinbase"
	Detail   string    `json:"detail"` // in the engine's own words
	JobID    string    `json:"job_id"`
	Required float64   `json:"required"` // the difficulty asked of it, where that applies
	Actual   float64   `json:"actual"`   // what the share was worth
}

// RejectionLog holds recent rejections for every worker.
type RejectionLog struct {
	mu       sync.Mutex
	byWorker map[string][]Rejection
}

func NewRejectionLog() *RejectionLog {
	return &RejectionLog{byWorker: map[string][]Rejection{}}
}

// Add records a rejection, keeping only the most recent few per worker. The
// worker is keyed by the part after the last dot, so a payout-prefixed name and
// a meshed miner's bare name are the same worker.
func (r *RejectionLog) Add(rj Rejection) {
	if r == nil {
		return
	}
	key := rj.Worker
	if i := strings.LastIndex(key, "."); i >= 0 {
		key = key[i+1:]
	}
	if key == "" {
		return
	}
	if rj.At.IsZero() {
		rj.At = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	list := append(r.byWorker[key], rj)
	if len(list) > rejectionsPerWorker {
		list = list[len(list)-rejectionsPerWorker:]
	}
	r.byWorker[key] = list
}

// ByWorker returns the recent rejections for every worker, newest last.
func (r *RejectionLog) ByWorker() map[string][]Rejection {
	if r == nil {
		return map[string][]Rejection{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]Rejection, len(r.byWorker))
	for w, list := range r.byWorker {
		out[w] = append([]Rejection(nil), list...)
	}
	return out
}
