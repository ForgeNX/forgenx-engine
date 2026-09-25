package coinapi

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store handles all SQLite persistence for coin app data.
type Store struct {
	mu          sync.Mutex
	db          *sql.DB
	maxHashrate map[string]float64 // in-memory peak pool hashrate per coin
}

func NewStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_journal=WAL&_timeout=10000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	s := &Store{db: db, maxHashrate: make(map[string]float64)}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			coin_symbol TEXT NOT NULL, height INTEGER NOT NULL,
			block_hash TEXT, block_time TEXT, miner_address TEXT, worker_name TEXT,
			share_difficulty REAL DEFAULT 0, reward REAL DEFAULT 0,
			shares_since_last INTEGER DEFAULT 0, luck_percent REAL DEFAULT 100.0,
			network_difficulty REAL DEFAULT 0,
			created_at TEXT NOT NULL, UNIQUE(coin_symbol, height))`,
		`CREATE INDEX IF NOT EXISTS idx_blocks_symbol ON blocks(coin_symbol)`,
		`CREATE TABLE IF NOT EXISTS pool_counters (
			coin_symbol TEXT PRIMARY KEY,
			last_blocks_found INTEGER DEFAULT 0, last_shares_accepted INTEGER DEFAULT 0,
			shares_offset INTEGER DEFAULT 0, shares_rejected_offset INTEGER DEFAULT 0,
			shares_stale_offset INTEGER DEFAULT 0, last_shares_rejected INTEGER DEFAULT 0,
			last_shares_stale INTEGER DEFAULT 0, uptime INTEGER DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS worker_shares (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			coin_symbol TEXT NOT NULL, worker_name TEXT NOT NULL,
			valid_shares INTEGER DEFAULT 0, invalid_shares INTEGER DEFAULT 0,
			stale_shares INTEGER DEFAULT 0, snapshot_time TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_ws_symbol_time ON worker_shares(coin_symbol, snapshot_time)`,
		`CREATE INDEX IF NOT EXISTS idx_ws_worker ON worker_shares(coin_symbol, worker_name, snapshot_time)`,
		`CREATE TABLE IF NOT EXISTS metric_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				coin_symbol TEXT NOT NULL,
				pool_hashrate_raw REAL DEFAULT 0,
				network_hashrate_raw REAL DEFAULT 0,
				difficulty REAL DEFAULT 0,
				recorded_at TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_samples_symbol_time ON metric_samples(coin_symbol, recorded_at)`,
		`CREATE TABLE IF NOT EXISTS worker_best_diff (
			coin_symbol TEXT NOT NULL, worker_name TEXT NOT NULL,
			best_all_time REAL DEFAULT 0, updated_at TEXT NOT NULL,
			last_seen TEXT, connected_at TEXT,
			shares_offset INTEGER DEFAULT 0, invalid_shares_offset INTEGER DEFAULT 0,
			last_difficulty REAL DEFAULT 0,
			PRIMARY KEY (coin_symbol, worker_name))`,
		// Which coin (or weighted split) the user has assigned a mesh miner to. Keyed
		// on the worker-name suffix, not the full authorize string, because the same
		// machine authorizes with a different payout prefix on each coin. Lives here
		// rather than in its own store so there is one database layer, even though
		// mesh assignment is not a coin-API concern.
		`CREATE TABLE IF NOT EXISTS mesh_assignments (
			worker TEXT PRIMARY KEY,
			allocation TEXT NOT NULL,
			updated_at TEXT NOT NULL)`,
	}
	// Migrate: add last_difficulty column if missing (safe to run multiple times)
	s.db.Exec(`ALTER TABLE pool_counters ADD COLUMN session_start INTEGER DEFAULT 0`)
	s.db.Exec(`ALTER TABLE pool_counters ADD COLUMN round_work REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE pool_counters ADD COLUMN round_shares INTEGER DEFAULT 0`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN worker_name TEXT`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN share_difficulty REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN reward REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN last_confirmations INTEGER DEFAULT -2`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN acknowledged INTEGER DEFAULT 0`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN network_difficulty REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE worker_best_diff ADD COLUMN last_difficulty REAL DEFAULT 0`)
	// Migrate: add best-share context columns if missing (safe to run multiple times)
	s.db.Exec(`ALTER TABLE worker_best_diff ADD COLUMN network_diff_at_best REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE worker_best_diff ADD COLUMN height_at_best INTEGER DEFAULT 0`)
	s.db.Exec(`ALTER TABLE worker_best_diff ADD COLUMN time_at_best TEXT`)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

// ── Pool Counters ─────────────────────────────────────────────────────────────

type PoolCounters struct {
	BlocksFound          int64
	SharesAccepted       int64
	SharesOffset         int64
	SharesRejectedOffset int64
	SharesStaleOffset    int64
	SharesRejected       int64
	SharesStale          int64
	Uptime               int64
	SessionStart         int64
	RoundWork            float64
	RoundShares          int64
}

func (s *Store) GetLastCounters(symbol string) PoolCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT last_blocks_found, last_shares_accepted,
		COALESCE(shares_offset,0), COALESCE(shares_rejected_offset,0),
		COALESCE(shares_stale_offset,0), COALESCE(last_shares_rejected,0),
		COALESCE(last_shares_stale,0), COALESCE(uptime,999999),
		COALESCE(session_start,0), COALESCE(round_work,0), COALESCE(round_shares,0)
		FROM pool_counters WHERE coin_symbol=?`, symbol)
	var c PoolCounters
	c.Uptime = 999999
	row.Scan(&c.BlocksFound, &c.SharesAccepted, &c.SharesOffset,
		&c.SharesRejectedOffset, &c.SharesStaleOffset,
		&c.SharesRejected, &c.SharesStale, &c.Uptime, &c.SessionStart, &c.RoundWork, &c.RoundShares)
	return c
}

// GetRoundWork returns the persisted accumulated round work for a coin (the
// share difficulty summed since the last block), used at startup to restore the
// in-memory effort accumulator across restarts. Returns 0 if none stored.
func (s *Store) GetRoundWork(symbol string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rw float64
	s.db.QueryRow(`SELECT COALESCE(round_work,0) FROM pool_counters WHERE coin_symbol=?`, symbol).Scan(&rw)
	return rw
}

// GetRoundShares returns the persisted per-round valid share count for a coin.
func (s *Store) GetRoundShares(symbol string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rs int64
	s.db.QueryRow(`SELECT COALESCE(round_shares,0) FROM pool_counters WHERE coin_symbol=?`, symbol).Scan(&rs)
	return rs
}

func (s *Store) UpdateCounters(symbol string, blocksFound, sharesAccepted,
	sharesOffset, sharesRejected, sharesRejectedOffset,
	sharesStale, sharesStaleOffset, uptime, sessionStart int64, roundWork float64, roundShares int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO pool_counters
		(coin_symbol, last_blocks_found, last_shares_accepted, shares_offset,
		last_shares_rejected, shares_rejected_offset,
		last_shares_stale, shares_stale_offset, uptime, session_start, round_work, round_shares)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(coin_symbol) DO UPDATE SET
		last_blocks_found=excluded.last_blocks_found,
		last_shares_accepted=excluded.last_shares_accepted,
		shares_offset=excluded.shares_offset,
		last_shares_rejected=excluded.last_shares_rejected,
		shares_rejected_offset=excluded.shares_rejected_offset,
		last_shares_stale=excluded.last_shares_stale,
		shares_stale_offset=excluded.shares_stale_offset,
		uptime=excluded.uptime,
		session_start=excluded.session_start,
		round_work=excluded.round_work,
		round_shares=excluded.round_shares`,
		symbol, blocksFound, sharesAccepted, sharesOffset,
		sharesRejected, sharesRejectedOffset,
		sharesStale, sharesStaleOffset, uptime, sessionStart, roundWork, roundShares)
	return err
}

// ── Worker Best Diff ──────────────────────────────────────────────────────────

type WorkerInfo struct {
	BestAllTime         float64
	LastSeen            string
	ConnectedAt         string
	SharesOffset        int64
	InvalidSharesOffset int64
	NetworkDiffAtBest   float64
	HeightAtBest        int64
	TimeAtBest          string
}

func (s *Store) GetWorkerBestDiffs(symbol string) map[string]WorkerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT worker_name, best_all_time,
		COALESCE(last_seen,''), COALESCE(connected_at,''),
		COALESCE(shares_offset,0), COALESCE(invalid_shares_offset,0),
		COALESCE(network_diff_at_best,0), COALESCE(height_at_best,0),
		COALESCE(time_at_best,'')
		FROM worker_best_diff WHERE coin_symbol=?`, symbol)
	if err != nil {
		return nil
	}
	defer rows.Close()
	result := make(map[string]WorkerInfo)
	for rows.Next() {
		var name string
		var info WorkerInfo
		if err := rows.Scan(&name, &info.BestAllTime, &info.LastSeen,
			&info.ConnectedAt, &info.SharesOffset, &info.InvalidSharesOffset,
			&info.NetworkDiffAtBest, &info.HeightAtBest, &info.TimeAtBest); err == nil {
			result[name] = info
		}
	}
	return result
}

// GetPoolBestAllTime returns the highest all-time best share difficulty across all workers for a coin.
func (s *Store) GetPoolBestAllTime(symbol string) (float64, string, string, bool) {
	// Must hold the mutex: without it, this read races concurrent share-submission
	// writes to worker_best_diff. Under contention (many miners) the unlocked read
	// intermittently failed and — with the error ignored — returned empty strings,
	// making best_all_time_worker/time blank for one poll and the all-time panel
	// flicker. The returned bool is false on any read error so the caller can keep
	// the previous value instead of publishing an empty one.
	s.mu.Lock()
	defer s.mu.Unlock()
	var best float64
	var worker, bestTime string
	err := s.db.QueryRow(`SELECT best_all_time, COALESCE(worker_name,''), COALESCE(time_at_best,'')
		FROM worker_best_diff WHERE coin_symbol=? ORDER BY best_all_time DESC LIMIT 1`,
		symbol).Scan(&best, &worker, &bestTime)
	if err != nil {
		return 0, "", "", false
	}
	return best, worker, bestTime, true
}

func (s *Store) UpdateWorkerBestDiff(symbol, workerName string, sessionBest, networkDiff float64, height uint32, bestTime string) (float64, float64, int64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// The context columns (network_diff_at_best, height_at_best, time_at_best)
	// are only overwritten when the incoming share is a NEW all-time best, so
	// the stored context always matches the stored best_all_time value.
	s.db.Exec(`INSERT INTO worker_best_diff
		(coin_symbol, worker_name, best_all_time, updated_at,
		 network_diff_at_best, height_at_best, time_at_best)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(coin_symbol, worker_name) DO UPDATE SET
		network_diff_at_best=CASE WHEN excluded.best_all_time > worker_best_diff.best_all_time
			THEN excluded.network_diff_at_best ELSE worker_best_diff.network_diff_at_best END,
		height_at_best=CASE WHEN excluded.best_all_time > worker_best_diff.best_all_time
			THEN excluded.height_at_best ELSE worker_best_diff.height_at_best END,
		time_at_best=CASE WHEN excluded.best_all_time > worker_best_diff.best_all_time
			THEN excluded.time_at_best ELSE worker_best_diff.time_at_best END,
		best_all_time=MAX(excluded.best_all_time, worker_best_diff.best_all_time),
		updated_at=excluded.updated_at`,
		symbol, workerName, sessionBest, now, networkDiff, height, bestTime)
	var best, storedNetDiff float64
	var storedHeight int64
	var storedTime string
	s.db.QueryRow(`SELECT best_all_time, COALESCE(network_diff_at_best,0), COALESCE(height_at_best,0), COALESCE(time_at_best,'')
		FROM worker_best_diff WHERE coin_symbol=? AND worker_name=?`,
		symbol, workerName).Scan(&best, &storedNetDiff, &storedHeight, &storedTime)
	return best, storedNetDiff, storedHeight, storedTime
}

func (s *Store) SetWorkerSharesOffset(symbol, workerName string, offset, invalidOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE worker_best_diff
		SET shares_offset=?, invalid_shares_offset=?
		WHERE coin_symbol=? AND worker_name=?`,
		offset, invalidOffset, symbol, workerName)
	return err
}

func (s *Store) RecordWorkerLastSeen(symbol, workerName, lastSeen, connectedAt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`INSERT INTO worker_best_diff
		(coin_symbol, worker_name, best_all_time, updated_at, last_seen, connected_at)
		VALUES (?,?,0,?,?,?)
		ON CONFLICT(coin_symbol, worker_name) DO UPDATE SET
		last_seen=excluded.last_seen, updated_at=excluded.updated_at,
		connected_at=COALESCE(excluded.connected_at, connected_at)`,
		symbol, workerName, now, lastSeen, connectedAt)
	return err
}

// ── Worker Shares ─────────────────────────────────────────────────────────────

type ShareCounts struct {
	Valid   int64
	Invalid int64
	Stale   int64
}

func (s *Store) RecordWorkerSnapshot(symbol, workerName string, valid, invalid, stale int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`INSERT INTO worker_shares
		(coin_symbol, worker_name, valid_shares, invalid_shares, stale_shares, snapshot_time)
		VALUES (?,?,?,?,?,?)`,
		symbol, workerName, valid, invalid, stale, now)
	if err == nil {
		s.db.Exec(`DELETE FROM worker_shares WHERE coin_symbol=? AND worker_name=?
			AND snapshot_time < datetime('now','-49 hours')`, symbol, workerName)
	}
	return err
}

// LastSeenByWorker returns when each worker was last mining, by the bare worker
// name - the part after the last dot, so a payout-prefixed name and a meshed
// miner's plain name are the same worker. Taken from the share snapshots, which
// are only written while a worker is submitting, so it survives an engine
// restart and says how long a miner has really been away.
var (
	lastSeenMu     sync.Mutex
	lastSeenCache  map[string]time.Time
	lastSeenCached time.Time
)

func (s *Store) LastSeenByWorker() map[string]time.Time {
	lastSeenMu.Lock()
	if lastSeenCache != nil && time.Since(lastSeenCached) < time.Minute {
		cached := lastSeenCache
		lastSeenMu.Unlock()
		return cached
	}
	lastSeenMu.Unlock()

	out := map[string]time.Time{}
	rows, err := s.db.Query(`SELECT worker_name, MAX(snapshot_time) FROM worker_shares GROUP BY worker_name`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var name, when string
		if err := rows.Scan(&name, &when); err != nil {
			continue
		}
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		t, err := time.Parse(time.RFC3339Nano, when)
		if err != nil || name == "" {
			continue
		}
		if prev, ok := out[name]; !ok || t.After(prev) {
			out[name] = t
		}
	}
	// A failed read leaves the previous answer in place rather than replacing it
	// with nothing: the table is busy often enough that one miss should not wipe
	// every miner's last-seen.
	lastSeenMu.Lock()
	if len(out) > 0 || lastSeenCache == nil {
		lastSeenCache, lastSeenCached = out, time.Now()
	}
	cached := lastSeenCache
	lastSeenMu.Unlock()
	return cached
}

func (s *Store) GetWorkerShares48hLive(symbol, workerName string, currentValid, currentInvalid int64) ShareCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT MIN(valid_shares), MIN(invalid_shares) FROM worker_shares
		WHERE coin_symbol=? AND worker_name=? AND snapshot_time >= datetime('now','-48 hours')`,
		symbol, workerName)
	var minValid, minInvalid sql.NullInt64
	row.Scan(&minValid, &minInvalid)
	if !minValid.Valid {
		return ShareCounts{}
	}
	v := currentValid - minValid.Int64
	i := currentInvalid - minInvalid.Int64
	if v < 0 {
		v = 0
	}
	if i < 0 {
		i = 0
	}
	return ShareCounts{Valid: v, Invalid: i}
}

func (s *Store) GetWorkerShares48h(symbol string) map[string]ShareCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT worker_name,
		MAX(valid_shares)-MIN(valid_shares),
		MAX(invalid_shares)-MIN(invalid_shares)
		FROM worker_shares
		WHERE coin_symbol=? AND snapshot_time >= datetime('now','-48 hours')
		GROUP BY worker_name`, symbol)
	if err != nil {
		return nil
	}
	defer rows.Close()
	result := make(map[string]ShareCounts)
	for rows.Next() {
		var name string
		var c ShareCounts
		if err := rows.Scan(&name, &c.Valid, &c.Invalid); err == nil {
			result[name] = c
		}
	}
	return result
}

func (s *Store) GetWorkerSharesAlltime(symbol, workerName string) ShareCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	var c ShareCounts
	s.db.QueryRow(`SELECT COALESCE(MAX(valid_shares),0), COALESCE(MAX(invalid_shares),0)
		FROM worker_shares WHERE coin_symbol=? AND worker_name=?`,
		symbol, workerName).Scan(&c.Valid, &c.Invalid)
	return c
}

// ── Blocks ────────────────────────────────────────────────────────────────────

type Block struct {
	ID                int64
	CoinSymbol        string
	Height            int64
	BlockHash         string
	BlockTime         string
	MinerAddress      string
	WorkerName        string
	ShareDifficulty   float64
	Reward            float64
	SharesSinceLast   int64
	LuckPercent       float64
	NetworkDifficulty float64
	CreatedAt         string
	Acknowledged      bool
}

func (s *Store) RecordBlock(symbol string, height int64, blockHash, blockTime, minerAddress, workerName string, shareDifficulty, reward float64, sharesSinceLast int64, luckPercent, networkDifficulty float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`INSERT OR IGNORE INTO blocks
		(coin_symbol, height, block_hash, block_time, miner_address, worker_name, share_difficulty, reward, shares_since_last, luck_percent, network_difficulty, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		symbol, height, blockHash, blockTime, minerAddress, workerName, shareDifficulty, reward, sharesSinceLast, luckPercent, networkDifficulty, now)
	return err
}

// UpdateBlockConfirmations stores the confirmation count for a block ONLY when
// it is a real answer (>= 0) that exceeds the stored high-water mark. Node-down
// sentinels (-2) and genuine-orphan (-1) never lower a previously-seen positive
// count, so a block that once matured stays matured across node restarts. A
// genuine orphan is recorded separately by the caller (it sets -1 explicitly via
// the status, not by lowering last_confirmations).
func (s *Store) UpdateBlockConfirmations(symbol string, height, conf int64) {
	if conf < 0 {
		return // -1 (orphan) / -2 (unavailable): don't overwrite the high-water mark
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Exec(`UPDATE blocks SET last_confirmations=?
		WHERE coin_symbol=? AND height=? AND ? > COALESCE(last_confirmations,-2)`,
		conf, symbol, height, conf)
}

// GetLastConfirmations returns the stored high-water-mark confirmation count for
// a block, or -2 if none recorded yet.
func (s *Store) GetLastConfirmations(symbol string, height int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var c int64 = -2
	s.db.QueryRow(`SELECT COALESCE(last_confirmations,-2) FROM blocks WHERE coin_symbol=? AND height=?`,
		symbol, height).Scan(&c)
	return c
}

// MarkBlockOrphaned records a genuine, node-confirmed orphan by setting
// last_confirmations = -1. This is the ONE place allowed to lower the stored
// confirmation value, because an orphan is terminal: the node has reported the
// block is on disk but not on the active chain. Callers MUST only invoke this
// when the node explicitly returned -1 (never on -2 / unavailable), otherwise a
// transient node hiccup could wrongly brand a valid block as orphaned.
func (s *Store) MarkBlockOrphaned(symbol string, height int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Exec(`UPDATE blocks SET last_confirmations=-1
		WHERE coin_symbol=? AND height=?`, symbol, height)
}

// AcknowledgeBlock marks a found/orphaned block as acknowledged by the user so
// the "Closest to block (session)" UI can exclude it and cascade to the next
// highest share. Purely a UI-state flag; does not affect counts or luck. Idempotent.
func (s *Store) AcknowledgeBlock(symbol string, height int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE blocks SET acknowledged=1
		WHERE coin_symbol=? AND height=?`, symbol, height)
	return err
}

// GetOrphanCount returns how many recorded blocks for a coin the node has
// confirmed as orphaned (last_confirmations = -1). Found blocks remain counted
// by GetBlockCount; this is a separate tally so the UI can show
// "Blocks Found: N, Orphaned: M".
func (s *Store) GetOrphanCount(symbol string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	s.db.QueryRow(`SELECT COUNT(*) FROM blocks WHERE coin_symbol=? AND COALESCE(last_confirmations,-2) = -1`, symbol).Scan(&count)
	return count
}

func (s *Store) GetBlocks(symbol string, limit int) ([]Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT id, coin_symbol, height,
		COALESCE(block_hash,''), COALESCE(block_time,''),
		COALESCE(miner_address,''), COALESCE(worker_name,''), COALESCE(share_difficulty,0), COALESCE(reward,0), shares_since_last, luck_percent, COALESCE(network_difficulty,0), created_at, COALESCE(acknowledged,0)
		FROM blocks WHERE coin_symbol=? ORDER BY height DESC LIMIT ?`, symbol, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []Block
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.ID, &b.CoinSymbol, &b.Height, &b.BlockHash,
			&b.BlockTime, &b.MinerAddress, &b.WorkerName, &b.ShareDifficulty, &b.Reward, &b.SharesSinceLast,
			&b.LuckPercent, &b.NetworkDifficulty, &b.CreatedAt, &b.Acknowledged); err == nil {
			blocks = append(blocks, b)
		}
	}
	return blocks, nil
}

func (s *Store) GetBlockCount(symbol string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	s.db.QueryRow(`SELECT COUNT(*) FROM blocks WHERE coin_symbol=?`, symbol).Scan(&count)
	return count
}

// GetLatestBlockTime returns the block_time of the most recent block for a coin
// (by height), or "" if none. Sourced from the durable blocks table so it stays
// correct across engine restarts (unlike the in-memory metrics counter).
func (s *Store) GetLatestBlockTime(symbol string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var t string
	s.db.QueryRow(`SELECT COALESCE(block_time,'') FROM blocks WHERE coin_symbol=? ORDER BY height DESC LIMIT 1`, symbol).Scan(&t)
	return t
}

// BlockExistsAtHeight reports whether a confirmed block is recorded at the given
// height for a coin. Used to classify a best-ratio share as a confirmed block.
func (s *Store) BlockExistsAtHeight(symbol string, height int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	s.db.QueryRow(`SELECT COUNT(*) FROM blocks WHERE coin_symbol=? AND height=?`, symbol, height).Scan(&count)
	return count > 0
}

type LuckStats struct {
	AvgLuck    float64
	Luckiest   float64
	Unluckiest float64
}

func (s *Store) GetLuckStats(symbol string) LuckStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stats LuckStats
	s.db.QueryRow(`SELECT COALESCE(AVG(luck_percent),0),
		COALESCE(MIN(luck_percent),0), COALESCE(MAX(luck_percent),0)
		FROM blocks WHERE coin_symbol=?`, symbol).Scan(
		&stats.AvgLuck, &stats.Luckiest, &stats.Unluckiest)
	return stats
}

func (s *Store) DeleteWorker(symbol, workerName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Exec(`DELETE FROM worker_shares WHERE coin_symbol=? AND worker_name=?`, symbol, workerName)
	s.db.Exec(`DELETE FROM worker_best_diff WHERE coin_symbol=? AND worker_name=?`, symbol, workerName)
	return nil
}

// ── Metric History ───────────────────────────────────────────────────────────

func (s *Store) UpdateAndGetMaxPoolHashrate(symbol string, current float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current > s.maxHashrate[symbol] {
		s.maxHashrate[symbol] = current
	}
	return s.maxHashrate[symbol]
}

func (s *Store) SaveWorkerDifficulty(symbol, workerName string, difficulty float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	s.db.Exec(`INSERT INTO worker_best_diff (coin_symbol, worker_name, best_all_time, updated_at, last_difficulty)
		VALUES (?, ?, 0, ?, ?)
		ON CONFLICT(coin_symbol, worker_name) DO UPDATE SET last_difficulty=excluded.last_difficulty, updated_at=excluded.updated_at`,
		symbol, workerName, now, difficulty)
}

func (s *Store) GetWorkerLastDifficulty(symbol, workerName string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var diff float64
	s.db.QueryRow(`SELECT COALESCE(last_difficulty,0) FROM worker_best_diff WHERE coin_symbol=? AND worker_name=?`,
		symbol, workerName).Scan(&diff)
	return diff
}

func (s *Store) GetMaxPoolHashrate(symbol string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxHashrate[symbol]
}

func (s *Store) RecordSample(symbol string, poolHashrate, networkHashrate, difficulty float64) error {
	s.mu.Lock()
	if poolHashrate > s.maxHashrate[symbol] {
		s.maxHashrate[symbol] = poolHashrate
	}
	s.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`INSERT INTO metric_samples
		(coin_symbol, pool_hashrate_raw, network_hashrate_raw, difficulty, recorded_at)
		VALUES (?,?,?,?,?)`,
		symbol, poolHashrate, networkHashrate, difficulty, now)
	if err == nil {
		// Prune samples older than 8 days
		s.db.Exec(`DELETE FROM metric_samples WHERE coin_symbol=? AND recorded_at < datetime('now','-8 days')`, symbol)
	}
	return err
}

// historyTrail maps trail label to (seconds, numPoints)
type historyTrail struct{ Seconds, Points int }

var historyTrails = map[string]historyTrail{
	"30m": {30 * 60, 30},
	"6h":  {6 * 3600, 72},
	"1d":  {24 * 3600, 96},
	"3d":  {3 * 24 * 3600, 72},
	"6d":  {6 * 24 * 3600, 144},
	"7d":  {7 * 24 * 3600, 168},
}

func (s *Store) GetHistory(symbol string, sinceSeconds, numPoints int, metric string) []float64 {
	validMetrics := map[string]bool{
		"pool_hashrate_raw": true, "network_hashrate_raw": true, "difficulty": true,
	}
	if !validMetrics[metric] {
		return make([]float64, numPoints)
	}

	cutoff := time.Now().UTC().Add(-time.Duration(sinceSeconds) * time.Second).Format(time.RFC3339Nano)
	s.mu.Lock()
	rows, err := s.db.Query(
		`SELECT recorded_at, `+metric+` FROM metric_samples
		 WHERE coin_symbol=? AND recorded_at >= ?
		 ORDER BY recorded_at ASC`, symbol, cutoff)
	s.mu.Unlock()

	if err != nil {
		return make([]float64, numPoints)
	}
	defer rows.Close()

	type row struct {
		recordedAt string
		value      float64
	}
	var data []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.recordedAt, &r.value); err == nil {
			data = append(data, r)
		}
	}

	if len(data) == 0 {
		return make([]float64, numPoints)
	}

	now := time.Now().UTC()
	start := now.Add(-time.Duration(sinceSeconds) * time.Second)
	bucketSec := float64(sinceSeconds) / float64(numPoints)

	buckets := make([][]float64, numPoints)
	for _, r := range data {
		ts, err := time.Parse(time.RFC3339Nano, r.recordedAt)
		if err != nil {
			ts, _ = time.Parse(time.RFC3339, r.recordedAt)
		}
		offset := ts.Sub(start).Seconds()
		idx := int(offset / bucketSec)
		if idx >= 0 && idx < numPoints {
			buckets[idx] = append(buckets[idx], r.value)
		}
	}

	result := make([]float64, numPoints)
	lastVal := 0.0
	for i, b := range buckets {
		if len(b) > 0 {
			sum := 0.0
			for _, v := range b {
				sum += v
			}
			lastVal = math.Round((sum/float64(len(b)))*10000) / 10000
		}
		result[i] = lastVal
	}
	return result
}

// The mesh default is stored as a single row in mesh_assignments under a reserved
// worker name. A separate table would be tidier, but this avoids a migration for
// one value, and the name cannot collide: a worker suffix comes from a miner's
// authorize string and can never contain a space.
const meshDefaultKey = "\x00 mesh default"

// The rotation interval is mesh-wide, stored under its own reserved key for the
// same reason as the default order: one value does not justify a table.
const meshIntervalKey = "\x00 mesh interval"

// GetMeshInterval returns how long a rotating miner spends on each cycle, as a
// Go duration string, and whether one has been set.
func (s *Store) GetMeshInterval() (string, bool) {
	return s.GetMeshAssignment(meshIntervalKey)
}

// SetMeshInterval records the rotation cycle length.
func (s *Store) SetMeshInterval(d string) error {
	return s.SetMeshAssignment(meshIntervalKey, d)
}

// Mesh-wide settings kept under reserved keys, like the default order.
const (
	meshNetworkKey    = "\x00 miner network"
	meshIncludeNewKey = "\x00 include new"
	meshSystemKey     = "\x00 system target"
)

// meshSortKey keeps how the Nexus tab sorts its miner list, e.g. "hashrate:desc".
const meshSortKey = "\x00 miner sort"

// GetMeshMinerSort returns the saved miner sort, by name ascending by default.
func (s *Store) GetMeshMinerSort() string {
	if v, ok := s.GetMeshAssignment(meshSortKey); ok && v != "" {
		return v
	}
	return "name:asc"
}

// SetMeshMinerSort records the miner sort.
func (s *Store) SetMeshMinerSort(v string) error {
	return s.SetMeshAssignment(meshSortKey, v)
}

// Automatic worker naming: whether a miner added to the mesh is renamed, and
// the prefix its name is built from.
const (
	meshAutoNameKey   = "\x00 auto name"
	meshNamePrefixKey = "\x00 name prefix"
)

// GetMeshAutoName reports whether a miner added to the mesh is renamed to the
// next free name in sequence. Off unless the user turns it on: renaming someone
// else's hardware is not something to do by default.
func (s *Store) GetMeshAutoName() bool {
	v, ok := s.GetMeshAssignment(meshAutoNameKey)
	return ok && v == "true"
}

// SetMeshAutoName records whether miners added to the mesh are renamed.
func (s *Store) SetMeshAutoName(v bool) error {
	return s.SetMeshAssignment(meshAutoNameKey, strconv.FormatBool(v))
}

// GetMeshNamePrefix returns the prefix automatic names are built from.
func (s *Store) GetMeshNamePrefix() string {
	v, _ := s.GetMeshAssignment(meshNamePrefixKey)
	return v
}

// SetMeshNamePrefix records the prefix automatic names are built from.
func (s *Store) SetMeshNamePrefix(v string) error {
	return s.SetMeshAssignment(meshNamePrefixKey, strings.TrimSpace(v))
}

// meshAddressKey is the address miners are pointed at when moved to the mesh.
// The engine cannot discover its own LAN address from inside its container, and
// the address the browser happens to be using may not be the one miners can
// reach, so the user confirms it.
const meshAddressKey = "\x00 mesh address"

// GetMeshAddress returns the address miners are pointed at for the mesh.
func (s *Store) GetMeshAddress() string {
	v, _ := s.GetMeshAssignment(meshAddressKey)
	return v
}

// SetMeshAddress records the address miners are pointed at for the mesh.
func (s *Store) SetMeshAddress(v string) error {
	return s.SetMeshAssignment(meshAddressKey, strings.TrimSpace(v))
}

// meshDiscoveredSortKey keeps how the Miners Discovered list is sorted.
const meshDiscoveredSortKey = "\x00 discovered sort"

// GetMeshDiscoveredSort returns the saved Miners Discovered sort.
func (s *Store) GetMeshDiscoveredSort() string {
	if v, ok := s.GetMeshAssignment(meshDiscoveredSortKey); ok && v != "" {
		return v
	}
	return "name:asc"
}

// SetMeshDiscoveredSort records the Miners Discovered sort.
func (s *Store) SetMeshDiscoveredSort(v string) error {
	return s.SetMeshAssignment(meshDiscoveredSortKey, v)
}

// meshPinsPrefix keys the coins pinned in the allocator, per worker and for Fleet
// Balance, so a pin holds across devices and reloads like every Nexus setting.
const meshPinsPrefix = "\x00 pins "

// GetMeshPins returns the coins pinned for a worker, or for Fleet Balance.
func (s *Store) GetMeshPins(worker string) []string {
	v, ok := s.GetMeshAssignment(meshPinsPrefix + worker)
	if !ok || v == "" {
		return []string{}
	}
	return strings.Split(v, ",")
}

// SetMeshPins records the pinned coins; an empty list clears them.
func (s *Store) SetMeshPins(worker string, coins []string) error {
	return s.SetMeshAssignment(meshPinsPrefix+worker, strings.Join(coins, ","))
}

// MeshAuto is the assignment value for a miner handed to the System Mesh: the
// balancer decides which coin it mines, towards the System Mesh target.
const MeshAuto = "AUTO"

// GetMeshSystemTarget returns the System Mesh's split, e.g. "DGB:60,BCH:40".
func (s *Store) GetMeshSystemTarget() (string, bool) {
	return s.GetMeshAssignment(meshSystemKey)
}

// SetMeshSystemTarget records the System Mesh's split.
func (s *Store) SetMeshSystemTarget(alloc string) error {
	return s.SetMeshAssignment(meshSystemKey, alloc)
}

// GetMeshNetwork returns the LAN range the engine scans for miners: a network,
// an address, or the first address of a range whose last is end.
func (s *Store) GetMeshNetwork() (start, end string) {
	v, ok := s.GetMeshAssignment(meshNetworkKey)
	if !ok {
		return "", ""
	}
	parts := strings.SplitN(v, "|", 2)
	start = parts[0]
	if len(parts) == 2 {
		end = parts[1]
	}
	return start, end
}

// SetMeshNetwork records the LAN range to scan.
func (s *Store) SetMeshNetwork(start, end string) error {
	return s.SetMeshAssignment(meshNetworkKey, strings.TrimSpace(start)+"|"+strings.TrimSpace(end))
}

// GetMeshIncludeNew reports whether a newly connected miner joins the System
// Mesh automatically. On by default: a new miner then joins the fleet balance
// straight away rather than waiting to be told.
func (s *Store) GetMeshIncludeNew() bool {
	v, ok := s.GetMeshAssignment(meshIncludeNewKey)
	return !ok || v != "false"
}

// SetMeshIncludeNew records whether new miners join the System Mesh.
func (s *Store) SetMeshIncludeNew(v bool) error {
	return s.SetMeshAssignment(meshIncludeNewKey, strconv.FormatBool(v))
}

// GetMeshDefault returns the coin the user chose for miners with no assignment
// of their own, and whether one has been set. Unset means the mesh falls back to
// its configured order.
func (s *Store) GetMeshDefault() (string, bool) {
	return s.GetMeshAssignment(meshDefaultKey)
}

// SetMeshDefault records the coin unassigned miners should bond to first.
func (s *Store) SetMeshDefault(allocation string) error {
	return s.SetMeshAssignment(meshDefaultKey, allocation)
}

// GetMeshAssignment returns the allocation string assigned to a mesh worker — a
// single coin ("DGB:100") or a weighted split ("DGB:50,BCH:50") — and whether one
// exists. A worker with no assignment mines the mesh default.
func (s *Store) GetMeshAssignment(worker string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var alloc string
	err := s.db.QueryRow(`SELECT allocation FROM mesh_assignments WHERE worker = ?`, worker).Scan(&alloc)
	if err != nil || alloc == "" {
		return "", false
	}
	return alloc, true
}

// SetMeshAssignment records the allocation for a worker, replacing any existing one.
func (s *Store) SetMeshAssignment(worker, allocation string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO mesh_assignments (worker, allocation, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(worker) DO UPDATE SET allocation = excluded.allocation, updated_at = excluded.updated_at`,
		worker, allocation, time.Now().UTC().Format(time.RFC3339))
	return err
}

// DeleteMeshAssignment clears a worker's assignment, returning it to the mesh default.
func (s *Store) DeleteMeshAssignment(worker string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM mesh_assignments WHERE worker = ?`, worker)
	return err
}

// ListMeshAssignments returns every assignment, keyed by worker.
func (s *Store) ListMeshAssignments() (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT worker, allocation FROM mesh_assignments`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var w, a string
		if err := rows.Scan(&w, &a); err == nil {
			if strings.HasPrefix(w, "\x00") { // every reserved key, present and future
				continue // mesh-wide settings, not workers
			}
			out[w] = a
		}
	}
	return out, rows.Err()
}

func (s *Store) Close() error { return s.db.Close() }
