package minerapi

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Scanner finds miners on the LAN and keeps a current reading for each, keyed by
// worker name. The engine cannot discover the LAN for itself — from inside its
// container it sees only Docker's networks — so the user names the range to scan.
//
// A full sweep runs every sweepEvery to find miners; the ones found are polled
// every pollEvery. A miner is matched to a worker by the username it mines under,
// taking the part after the last dot, so "address.Ellevix004" and a meshed
// miner's plain "Ellevix002" both match their workers.
type Scanner struct {
	mu       sync.Mutex
	hosts    []netip.Addr
	miners   map[string]string  // host -> worker, for miners found
	readings map[string]Reading // worker -> latest reading
	seen     map[string]time.Time
	rescan   chan struct{}
}

const (
	sweepEvery    = 10 * time.Minute
	pollEvery     = 30 * time.Second
	sweepParallel = 32
	sweepTimeout  = 4 * time.Second // per address during discovery
	staleAfter    = 2 * time.Minute
	maxAddresses  = 256
)

func NewScanner() *Scanner {
	return &Scanner{
		miners:   map[string]string{},
		readings: map[string]Reading{},
		seen:     map[string]time.Time{},
		rescan:   make(chan struct{}, 1),
	}
}

// ParseRange turns the user's setting into addresses. start may be a network
// ("192.168.1.0/24"), a single address (meaning its /24), or, with end, the
// first address of an inclusive range. At most maxAddresses are returned, so a
// mistyped setting cannot become a scan of thousands.
func ParseRange(start, end string) ([]netip.Addr, error) {
	start, end = strings.TrimSpace(start), strings.TrimSpace(end)
	if start == "" {
		return nil, nil
	}
	var first, last netip.Addr
	if strings.Contains(start, "/") {
		p, err := netip.ParsePrefix(start)
		if err != nil {
			return nil, fmt.Errorf("start: %w", err)
		}
		if p.Bits() < 24 {
			return nil, fmt.Errorf("start: %s is larger than a /24", start)
		}
		p = p.Masked()
		first = p.Addr()
		last = first
		for n := 1; n < 1<<(32-p.Bits()); n++ {
			last = last.Next()
		}
	} else {
		a, err := netip.ParseAddr(start)
		if err != nil {
			return nil, fmt.Errorf("start: %w", err)
		}
		if end == "" {
			p, _ := a.Prefix(24)
			first = p.Addr()
			last = first
			for n := 1; n < 256; n++ {
				last = last.Next()
			}
		} else {
			b, err := netip.ParseAddr(end)
			if err != nil {
				return nil, fmt.Errorf("end: %w", err)
			}
			if b.Less(a) {
				return nil, fmt.Errorf("end %s is before start %s", end, start)
			}
			first, last = a, b
		}
	}
	if !first.Is4() || !last.Is4() {
		return nil, fmt.Errorf("only IPv4 ranges are supported")
	}
	var out []netip.Addr
	for a := first; ; a = a.Next() {
		out = append(out, a)
		if a == last || len(out) >= maxAddresses {
			break
		}
	}
	return out, nil
}

// SetRange replaces the range to scan and triggers a fresh sweep.
func (s *Scanner) SetRange(start, end string) error {
	hosts, err := ParseRange(start, end)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.hosts = hosts
	s.mu.Unlock()
	select {
	case s.rescan <- struct{}{}:
	default:
	}
	return nil
}

// WorkerOf returns the worker name a pool username refers to.
func WorkerOf(poolUser string) string {
	if i := strings.LastIndex(poolUser, "."); i >= 0 {
		return poolUser[i+1:]
	}
	return poolUser
}

// Readings returns the current reading for every miner seen recently, keyed by
// worker name. A miner not heard from in staleAfter is left out rather than
// reported with old figures.
func (s *Scanner) Readings() map[string]Reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Reading, len(s.readings))
	for w, r := range s.readings {
		if time.Since(s.seen[w]) <= staleAfter {
			out[w] = r
		}
	}
	return out
}

func (s *Scanner) record(host string, r Reading) {
	r.Host = host
	w := WorkerOf(r.PoolUser)
	if w == "" {
		return
	}
	s.mu.Lock()
	s.miners[host] = w
	s.readings[w] = r
	s.seen[w] = time.Now()
	s.mu.Unlock()
}

// Run sweeps and polls until stop is closed.
func (s *Scanner) Run(stop <-chan struct{}) {
	sweep := time.NewTicker(sweepEvery)
	poll := time.NewTicker(pollEvery)
	defer sweep.Stop()
	defer poll.Stop()
	s.sweep()
	for {
		select {
		case <-stop:
			return
		case <-s.rescan:
			s.sweep()
		case <-sweep.C:
			s.sweep()
		case <-poll.C:
			s.poll()
		}
	}
}

func (s *Scanner) sweep() {
	s.mu.Lock()
	hosts := append([]netip.Addr(nil), s.hosts...)
	s.mu.Unlock()
	if len(hosts) == 0 {
		return
	}
	sem := make(chan struct{}, sweepParallel)
	var wg sync.WaitGroup
	for _, h := range hosts {
		host := h.String()
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), sweepTimeout)
			defer cancel()
			if r, err := Probe(ctx, host, ""); err == nil {
				s.record(host, r)
			}
		}()
	}
	wg.Wait()
}

func (s *Scanner) poll() {
	s.mu.Lock()
	hosts := make([]string, 0, len(s.miners))
	for h := range s.miners {
		hosts = append(hosts, h)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, host := range hosts {
		host := host
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, err := Probe(context.Background(), host, ""); err == nil {
				s.record(host, r)
			}
		}()
	}
	wg.Wait()
}
