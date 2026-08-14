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

	mu            sync.Mutex
	extranonce1   string
	extranonce2sz int
	subscribed    bool
	authorized    bool

	onMessage func(line []byte)
}

// NewBackend creates (but does not connect) a backend for one coin.
func NewBackend(symbol, addr, payout, worker string, logger *logging.Logger) *Backend {
	return &Backend{Symbol: symbol, Addr: addr, Payout: payout, Worker: worker, logger: logger}
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
	b.conn = conn
	b.reader = bufio.NewReader(conn)

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

	authUser := b.Payout
	if b.Worker != "" {
		authUser = b.Payout + "." + b.Worker
	}
	if err := b.send(map[string]interface{}{
		"id": 2, "method": "mining.authorize", "params": []interface{}{authUser, "x"},
	}); err != nil {
		return fmt.Errorf("authorize send: %w", err)
	}
	b.mu.Lock()
	b.authorized = true
	b.mu.Unlock()

	b.logger.Info("[nexus] backend %s connected: %s (payout=%s.%s en1=%s en2sz=%d)",
		b.Symbol, b.Addr, b.Payout, b.Worker, b.extranonce1, b.extranonce2sz)
	return nil
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

// Run reads from the coin backend and forwards each line via onMessage.
func (b *Backend) Run() {
	for {
		line, err := b.readLine()
		if err != nil {
			b.logger.Debug("[nexus] backend %s read end: %v", b.Symbol, err)
			return
		}
		if b.onMessage != nil {
			b.onMessage(line)
		}
	}
}

// SendRaw writes a raw JSON line to the coin backend.
func (b *Backend) SendRaw(line []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil {
		return fmt.Errorf("backend %s not connected", b.Symbol)
	}
	b.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := b.conn.Write(append(line, '\n'))
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
	line, err := b.reader.ReadBytes('\n')
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
