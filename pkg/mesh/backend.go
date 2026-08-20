package mesh

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
)

// Backend is a connection to one coin's real V1 stratum server. Nexus dials it
// as an ordinary client: it subscribes, authorizes with the coin's own payout
// address, then relays messages between the coin and the miner. The coin server
// does all the real work (vardiff, block construction, worker tracking).
type Backend struct {
	Symbol string
	Addr   string // host:port of the coin's V1 stratum
	Payout string // coin's configured payout address (from Settings)
	Worker string // worker suffix (e.g. "Ellevix002")

	conn   net.Conn
	reader *bufio.Reader
	logger *logging.Logger

	mu sync.Mutex
	// writeMu serialises socket writes. Separate from mu on purpose: holding the
	// state mutex across a write means a coin that stops reading blocks every
	// other goroutine touching this backend, and a reconnect could not swap the
	// connection while a write was stuck.
	writeMu       sync.Mutex
	extranonce1   string
	extranonce2sz int
	subscribed    bool
	authorized    bool

	// Latest control state from the coin, captured so we can replay it to the
	// miner as soon as it is ready (avoids a race where jobs arrive before the
	// miner has subscribed).
	lastSetDifficulty []byte // raw mining.set_difficulty line
	lastNotify        []byte // raw mining.notify line

	// live gates backend->miner forwarding. While false, messages are captured
	// (latest difficulty/job) but NOT forwarded. The relay flips it true once the
	// miner has authorized, then replays lastSetDifficulty + lastNotify.
	live bool

	// firstJob is closed once the backend has captured DGB's first mining.notify,
	// so Connect can wait for a job to be ready before the miner starts (avoids a
	// race where the miner authorizes before any job exists -> stalls/reconnects).
	// Guarded by mu so a reconnect can re-arm it: after a coin comes back the
	// backend has no job again, and callers must block until the new connection
	// produces one rather than seeing the previous connection's closed channel.
	firstJob chan struct{}

	// dead is set when Run() exits — the coin closed the connection, restarted, or
	// the link dropped. Nothing else notices otherwise: a warm backend would sit
	// silently unusable until rotation switched onto it, and an active one would
	// leave the miner receiving no jobs with no error to explain it.
	dead bool

	// versionMask is the version-rolling mask the coin granted this connection
	// (from mining.configure). The relay hands this same mask to the miner so it
	// only rolls bits the coin will accept.
	versionMask string

	// resolve re-reads the coin's endpoint before each reconnect attempt. A coin
	// that was stopped may come back on a different port, and one that was never
	// running when the miner bonded has no address at all until it starts, so the
	// address cannot be fixed at construction.
	resolve func() (addr string, payout string, running bool)

	onMessage func(line []byte)

	// onDead, if set, is called once when Run() exits. The relay uses it to tear
	// down the miner session when the ACTIVE backend dies, so the miner reconnects
	// and re-bonds instead of sitting on a connection that will never send another
	// job. A warm backend dying is not fatal: rotation skips it via Alive().
	onDead func()
}

// NewBackend creates (but does not connect) a backend for one coin.
func NewBackend(symbol, addr, payout, worker string, logger *logging.Logger) *Backend {
	return &Backend{Symbol: symbol, Addr: addr, Payout: payout, Worker: worker, logger: logger, firstJob: make(chan struct{})}
}

// Connect dials the coin's V1 stratum, subscribes, and authorizes with the coin
// payout address, capturing extranonce1/size so the miner adopts the same values.
func (b *Backend) Connect() error {
	conn, err := net.DialTimeout("tcp", b.Addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s (%s): %w", b.Addr, b.Symbol, err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}
	b.mu.Lock()
	b.conn = conn
	b.reader = bufio.NewReader(conn)
	b.mu.Unlock()

	// Negotiate version-rolling FIRST. Miners like the Bitaxe roll the block
	// version (ASICBoost); the coin must enable version-rolling on THIS connection
	// or it validates rolled shares against a zero mask and silently drops them.
	if err := b.send(map[string]interface{}{
		"id": 0, "method": "mining.configure",
		"params": []interface{}{
			[]interface{}{"version-rolling"},
			map[string]interface{}{"version-rolling.mask": "ffffffff"},
		},
	}); err != nil {
		return fmt.Errorf("configure send: %w", err)
	}
	cfgResp, err := b.readLine()
	if err != nil {
		return fmt.Errorf("configure read: %w", err)
	}
	b.parseConfigure(cfgResp)

	if err := b.send(map[string]interface{}{
		"id": 1, "method": "mining.subscribe", "params": []interface{}{"nexus-relay/1.0"},
	}); err != nil {
		return fmt.Errorf("subscribe send: %w", err)
	}
	subResp, err := b.readLine()
	if err != nil {
		return fmt.Errorf("subscribe read: %w", err)
	}
	if err := b.parseSubscribe(subResp); err != nil {
		return fmt.Errorf("subscribe parse: %w", err)
	}

	en1, en2sz := b.Extranonce()
	b.logger.Info("[nexus] backend %s connected: %s (en1=%s en2sz=%d)",
		b.Symbol, b.Addr, en1, en2sz)
	return nil
}

func (b *Backend) Authorize(worker string) error {
	b.mu.Lock()
	b.Worker = worker
	b.mu.Unlock()
	authUser := b.Payout
	if worker != "" {
		authUser = b.Payout + "." + worker
	}
	if err := b.send(map[string]interface{}{
		"id": 2, "method": "mining.authorize", "params": []interface{}{authUser, "x"},
	}); err != nil {
		return fmt.Errorf("authorize send: %w", err)
	}
	b.mu.Lock()
	b.authorized = true
	b.mu.Unlock()
	b.logger.Info("[nexus] backend %s authorized as %s.%s", b.Symbol, b.Payout, worker)
	return nil
}

// parseConfigure reads the coin's mining.configure response and records the
// granted version-rolling mask (if any).
func (b *Backend) parseConfigure(line []byte) {
	var resp struct {
		Result map[string]interface{} `json:"result"`
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return
	}
	if resp.Result == nil {
		return
	}
	if vr, ok := resp.Result["version-rolling"].(bool); ok && vr {
		if mask, ok := resp.Result["version-rolling.mask"].(string); ok {
			b.mu.Lock()
			b.versionMask = mask
			b.mu.Unlock()
		}
	}
}

// VersionMask returns the version-rolling mask the coin granted (empty if none).
func (b *Backend) VersionMask() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.versionMask
}

func (b *Backend) parseSubscribe(line []byte) error {
	var resp struct {
		Result []json.RawMessage `json:"result"`
		Error  json.RawMessage   `json:"error"`
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return err
	}
	if len(resp.Result) < 3 {
		return fmt.Errorf("unexpected subscribe result: %s", string(line))
	}
	var en1 string
	if err := json.Unmarshal(resp.Result[1], &en1); err != nil {
		return fmt.Errorf("extranonce1: %w", err)
	}
	var en2sz int
	if err := json.Unmarshal(resp.Result[2], &en2sz); err != nil {
		return fmt.Errorf("extranonce2_size: %w", err)
	}
	b.mu.Lock()
	b.extranonce1 = en1
	b.extranonce2sz = en2sz
	b.subscribed = true
	b.mu.Unlock()
	return nil
}

// Extranonce returns the coin's assigned extranonce1 and extranonce2 size.
func (b *Backend) Extranonce() (string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.extranonce1, b.extranonce2sz
}

// Run reads from the coin backend, captures the latest difficulty/job, and
// forwards messages to the miner once the backend is live. Responses to the
// backend's own subscribe/authorize (id 1/2) are swallowed — the miner never
// sent those and must not see them.
func (b *Backend) Run() {
	for {
		line, err := b.readLine()
		if err != nil {
			b.logger.Info("[nexus] backend %s connection ended: %v", b.Symbol, err)
			// Only the alive->dead transition is an event. Once reconnecting, Run is
			// started and stopped repeatedly by the reconnect loop — every failed
			// attempt would otherwise look like a fresh death and re-trigger failover
			// on a session that has already moved on.
			b.mu.Lock()
			wasAlive := !b.dead
			b.dead = true
			onDead := b.onDead
			b.mu.Unlock()
			if wasAlive && onDead != nil {
				onDead()
			}
			return
		}
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(line, &msg)

		// Swallow the backend's own subscribe/authorize responses (id 1 or 2,
		// no method). The miner issued its own subscribe/authorize which the
		// relay answered locally.
		if msg.Method == "" && len(msg.ID) > 0 {
			idStr := string(msg.ID)
			if idStr == "1" || idStr == "2" {
				continue
			}
		}

		// Capture latest control state for replay.
		switch msg.Method {
		case "mining.set_difficulty":
			b.mu.Lock()
			b.lastSetDifficulty = append([]byte(nil), line...)
			b.mu.Unlock()
		case "mining.notify":
			b.mu.Lock()
			b.lastNotify = append([]byte(nil), line...)
			b.mu.Unlock()
			b.signalFirstJob()
		case "mining.ping":
			// Answer the coin's keepalive ourselves rather than forwarding it. The V1
			// server closes any session it has not READ from within IdleTimeout (5
			// minutes), and a warm backend — one bonded to a coin the miner is not
			// currently mining — sends nothing at all. Without this reply every
			// non-active backend is dropped a few minutes after bonding, and rotation
			// would switch the miner onto a dead connection.
			if err := b.send(map[string]interface{}{
				"id": rawOrNull(msg.ID), "result": "pong", "error": nil,
			}); err != nil {
				b.logger.Debug("[nexus] backend %s pong failed: %v", b.Symbol, err)
			}
			continue
		}

		b.mu.Lock()
		live := b.live
		b.mu.Unlock()
		if live && b.onMessage != nil {
			b.onMessage(line)
		}
	}
}

// reconnectInterval bounds how often a dead backend redials its coin. Starts short
// so a brief blip is picked up quickly, backs off so a coin that is down for hours
// is not hammered.
const (
	reconnectMin = 15 * time.Second
	reconnectMax = 2 * time.Minute
)

// Reconnect keeps a dead backend trying to come back. It owns the connection
// lifecycle for the backend after the initial Connect: it redials, re-authorizes
// with the worker name the miner gave, waits for the coin to actually produce a
// job, and only then clears dead and starts a fresh Run.
//
// Waiting for a job before clearing dead matters: a node that has just restarted
// accepts stratum connections long before it is synced and building templates. A
// backend that reported itself alive at that point would be a failback candidate
// the miner could be moved onto, only to sit there receiving nothing.
//
// Run is started from here and nowhere else once reconnecting, so there is never
// more than one goroutine reading the backend's socket.
func (b *Backend) Reconnect(stop func() bool) {
	delay := reconnectMin
	for {
		time.Sleep(delay)
		if stop() {
			return
		}
		if !b.Alive() {
			b.mu.Lock()
			resolve := b.resolve
			b.mu.Unlock()
			if resolve != nil {
				addr, payout, running := resolve()
				if !running || addr == "" {
					// Coin still down, or not yet started. Keep waiting rather than
					// dialling an address that is not listening.
					delay = nextDelay(delay)
					continue
				}
				b.mu.Lock()
				b.Addr = addr
				if payout != "" {
					b.Payout = payout
				}
				b.mu.Unlock()
			}
			b.rearmFirstJob()
			if err := b.Connect(); err != nil {
				b.logger.Debug("[nexus] backend %s redial failed: %v", b.Symbol, err)
				delay = nextDelay(delay)
				continue
			}
			worker := b.currentWorker()
			if worker != "" {
				if err := b.Authorize(worker); err != nil {
					b.logger.Debug("[nexus] backend %s reauthorize failed: %v", b.Symbol, err)
					b.Close()
					delay = nextDelay(delay)
					continue
				}
			}
			// Read from the new connection so jobs can arrive, then wait for one.
			go b.Run()
			if !b.WaitForFirstJob(30 * time.Second) {
				b.logger.Debug("[nexus] backend %s reconnected but produced no job; still catching up", b.Symbol)
				b.Close() // ends the Run we just started
				delay = nextDelay(delay)
				continue
			}
			b.mu.Lock()
			b.dead = false
			b.mu.Unlock()
			b.logger.Info("[nexus] backend %s reconnected and producing jobs", b.Symbol)
			delay = reconnectMin
		}
	}
}

func nextDelay(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectMax {
		d = reconnectMax
	}
	return d
}

// currentWorker returns the worker name this backend last authorized with, so a
// reconnect can re-establish the same identity on the coin.
func (b *Backend) currentWorker() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Worker
}

// markDead puts a backend into the state Run leaves it in when a connection ends,
// without having had a connection at all. Used when a coin is configured but not
// serving at bond time: the backend joins the session as a dead one its reconnect
// loop will bring up, rather than being left out and never watched for.
func (b *Backend) markDead() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dead = true
}

// SetResolver installs the callback Reconnect uses to find the coin's current
// endpoint. Without one, Reconnect redials the address the backend was built with.
func (b *Backend) SetResolver(f func() (string, string, bool)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resolve = f
}

// signalFirstJob marks that this connection has produced a job. Safe to call on
// every notify: only the first close per connection has any effect.
func (b *Backend) signalFirstJob() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.firstJob:
		// already signalled for this connection
	default:
		close(b.firstJob)
	}
}

// rearmFirstJob replaces the signal channel before a reconnect attempt, so waiters
// block on the new connection rather than the previous one's completed signal.
func (b *Backend) rearmFirstJob() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.firstJob = make(chan struct{})
}

// WaitForFirstJob blocks until the backend has captured the coin's first job
// (mining.notify) or the timeout elapses. This guarantees a job is cached before
// the miner starts, so GoLive always has real work to replay.
func (b *Backend) WaitForFirstJob(timeout time.Duration) bool {
	b.mu.Lock()
	ch := b.firstJob
	b.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// GoLive flips the backend to forwarding mode and returns the latest
// set_difficulty and notify lines (if any) so the relay can replay them to the
// miner immediately.
func (b *Backend) GoLive() (setDiff, notify []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.live = true
	return b.lastSetDifficulty, b.lastNotify
}

// SendRaw writes a raw JSON line to the coin backend.
func (b *Backend) SendRaw(line []byte) error {
	b.mu.Lock()
	c := b.conn
	b.mu.Unlock()
	if c == nil {
		return fmt.Errorf("backend %s not connected", b.Symbol)
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.Write(append(line, '\n'))
	return err
}

func (b *Backend) send(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.SendRaw(data)
}

func (b *Backend) readLine() ([]byte, error) {
	b.mu.Lock()
	r := b.reader
	b.mu.Unlock()
	if r == nil {
		return nil, fmt.Errorf("backend %s not connected", b.Symbol)
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line, nil
}

// Close shuts the backend connection.
func (b *Backend) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
}

// Alive reports whether this backend's connection to the coin is still usable.
// Rotation must check it before switching a miner onto a backend, and the relay
// checks it for the active backend so a dropped upstream surfaces as a miner
// disconnect rather than silence.
func (b *Backend) Alive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.dead
}
