package coinapi

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Worker names assigned automatically when a miner is added to the mesh. The
// next free name in a sequence - Worker019, Worker020 - counted against every
// name the engine knows about, so one is never given out twice.
//
// A name handed out is remembered for a while, because the miner it was given to
// takes a minute to restart and reconnect under it. Without that, adding three
// miners in a row would give them all the same name.
const reservedNameFor = time.Hour

var numberedName = regexp.MustCompile(`^(.*?)(\d+)$`)

type nameReservations struct {
	mu   sync.Mutex
	held map[string]time.Time
}

func newNameReservations() *nameReservations {
	return &nameReservations{held: map[string]time.Time{}}
}

func (r *nameReservations) reserve(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held[strings.ToLower(name)] = time.Now()
}

func (r *nameReservations) current() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]bool{}
	for n, at := range r.held {
		if time.Since(at) > reservedNameFor {
			delete(r.held, n)
			continue
		}
		out[n] = true
	}
	return out
}

// nextWorkerName returns the first free name for a prefix, given every name
// already taken. Numbering follows the highest existing number for that prefix,
// so a fleet at Worker019 continues at Worker020 rather than filling gaps -
// reusing an old number would make two miners' history hard to tell apart.
func nextWorkerName(prefix string, taken map[string]bool) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	width, highest := 3, 0
	lower := strings.ToLower(prefix)
	for name := range taken {
		m := numberedName.FindStringSubmatch(name)
		if m == nil || !strings.EqualFold(m[1], lower) {
			continue
		}
		n, err := strconv.Atoi(m[2])
		if err != nil {
			continue
		}
		if n > highest {
			highest = n
		}
		if len(m[2]) > width {
			width = len(m[2])
		}
	}
	for n := highest + 1; n < highest+1000; n++ {
		candidate := fmt.Sprintf("%s%0*d", prefix, width, n)
		if !taken[strings.ToLower(candidate)] {
			return candidate
		}
	}
	return ""
}

// takenWorkerNames gathers every name the engine knows to be in use: miners on
// the mesh, miners the scanner has found, names with a stored assignment, and
// names handed out recently.
func (c *CoinAPI) takenWorkerNames() map[string]bool {
	taken := map[string]bool{}
	add := func(n string) {
		if n = strings.TrimSpace(n); n != "" {
			taken[strings.ToLower(n)] = true
		}
	}
	if c.meshActiveCoins != nil {
		for worker := range c.meshActiveCoins() {
			add(worker)
		}
	}
	if c.scanner != nil {
		for worker := range c.scanner.Readings() {
			add(worker)
		}
	}
	if assignments, err := c.store.ListMeshAssignments(); err == nil {
		for worker := range assignments {
			if !strings.HasPrefix(worker, "\x00") {
				add(worker)
			}
		}
	}
	for n := range c.nameReservations.current() {
		taken[n] = true
	}
	return taken
}

// NextWorkerName returns the name the next miner added to the mesh would be
// given, or empty when automatic naming is off or has no prefix.
func (c *CoinAPI) NextWorkerName() string {
	if !c.store.GetMeshAutoName() {
		return ""
	}
	return nextWorkerName(c.store.GetMeshNamePrefix(), c.takenWorkerNames())
}
