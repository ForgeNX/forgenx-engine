package mesh

import (
	"sync"
	"time"
)

// What the mesh has been doing lately, so the tab can show it rather than
// leaving it in the log where nobody looks. The last fifty events, in memory:
// enough to see what has happened recently, and honest about not being a
// durable record - a restart clears it, as it clears the session's counters.
const activityKept = 50

// Event kinds. The UI filters on these, so they are part of the contract.
const (
	ActivitySwitch   = "switch"   // a miner moved from one node to another
	ActivityBalancer = "balancer" // Fleet Balance placed or moved a miner
	ActivityJoin     = "join"     // a miner bonded to the mesh
	ActivityLeave    = "leave"    // a miner's session ended
)

// Activity is one thing the mesh did.
type Activity struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	Worker string    `json:"worker"`
	From   string    `json:"from,omitempty"` // the node it left, where that applies
	To     string    `json:"to,omitempty"`   // the node it went to
	Detail string    `json:"detail,omitempty"`
}

type activityLog struct {
	mu     sync.Mutex
	events []Activity
}

// note records an event, keeping only the most recent few.
func (m *Mesh) note(kind, worker, from, to, detail string) {
	if worker == "" {
		return
	}
	m.activity.mu.Lock()
	defer m.activity.mu.Unlock()
	m.activity.events = append(m.activity.events, Activity{
		At: time.Now(), Kind: kind, Worker: worker, From: from, To: to, Detail: detail,
	})
	if len(m.activity.events) > activityKept {
		m.activity.events = m.activity.events[len(m.activity.events)-activityKept:]
	}
}

// Activity returns what the mesh has done recently, newest first.
func (m *Mesh) Activity() []Activity {
	m.activity.mu.Lock()
	defer m.activity.mu.Unlock()
	out := make([]Activity, len(m.activity.events))
	for i, e := range m.activity.events {
		out[len(m.activity.events)-1-i] = e
	}
	return out
}
