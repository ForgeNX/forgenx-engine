package coinapi

import (
	"database/sql"
	"fmt"
	"math"
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
	}
	// Migrate: add last_difficulty column if missing (safe to run multiple times)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN worker_name TEXT`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN share_difficulty REAL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE blocks ADD COLUMN reward REAL DEFAULT 0`)
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
}

func (s *Store) GetLastCounters(symbol string) PoolCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT last_blocks_found, last_shares_accepted,
		COALESCE(shares_offset,0), COALESCE(shares_rejected_offset,0),
		COALESCE(shares_stale_offset,0), COALESCE(last_shares_rejected,0),
		COALESCE(last_shares_stale,0), COALESCE(uptime,999999)
		FROM pool_counters WHERE coin_symbol=?`, symbol)
	var c PoolCounters
	c.Uptime = 999999
	row.Scan(&c.BlocksFound, &c.SharesAccepted, &c.SharesOffset,
		&c.SharesRejectedOffset, &c.SharesStaleOffset,
		&c.SharesRejected, &c.SharesStale, &c.Uptime)
	return c
}

func (s *Store) UpdateCounters(symbol string, blocksFound, sharesAccepted,
	sharesOffset, sharesRejected, sharesRejectedOffset,
	sharesStale, sharesStaleOffset, uptime int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO pool_counters
		(coin_symbol, last_blocks_found, last_shares_accepted, shares_offset,
		last_shares_rejected, shares_rejected_offset,
		last_shares_stale, shares_stale_offset, uptime)
		VALUES (?,?,?,?,?,?,?,?,?)
		ON CONFLICT(coin_symbol) DO UPDATE SET
		last_blocks_found=excluded.last_blocks_found,
		last_shares_accepted=excluded.last_shares_accepted,
		shares_offset=excluded.shares_offset,
		last_shares_rejected=excluded.last_shares_rejected,
		shares_rejected_offset=excluded.shares_rejected_offset,
		last_shares_stale=excluded.last_shares_stale,
		shares_stale_offset=excluded.shares_stale_offset,
		uptime=excluded.uptime`,
		symbol, blocksFound, sharesAccepted, sharesOffset,
		sharesRejected, sharesRejectedOffset,
		sharesStale, sharesStaleOffset, uptime)
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
func (s *Store) GetPoolBestAllTime(symbol string) float64 {
	var best float64
	s.db.QueryRow(`SELECT COALESCE(MAX(best_all_time), 0) FROM worker_best_diff WHERE coin_symbol=?`, symbol).Scan(&best)
	return best
}

func (s *Store) UpdateWorkerBestDiff(symbol, workerName string, sessionBest, networkDiff float64, height uint32, bestTime string) float64 {
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
	var best float64
	s.db.QueryRow(`SELECT best_all_time FROM worker_best_diff WHERE coin_symbol=? AND worker_name=?`,
		symbol, workerName).Scan(&best)
	return best
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
	ID              int64
	CoinSymbol      string
	Height          int64
	BlockHash       string
	BlockTime       string
	MinerAddress    string
	WorkerName      string
	ShareDifficulty float64
	Reward          float64
	SharesSinceLast int64
	LuckPercent     float64
	CreatedAt       string
}

func (s *Store) RecordBlock(symbol string, height int64, blockHash, blockTime, minerAddress, workerName string, shareDifficulty, reward float64, sharesSinceLast int64, luckPercent float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`INSERT OR IGNORE INTO blocks
		(coin_symbol, height, block_hash, block_time, miner_address, worker_name, share_difficulty, reward, shares_since_last, luck_percent, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		symbol, height, blockHash, blockTime, minerAddress, workerName, shareDifficulty, reward, sharesSinceLast, luckPercent, now)
	return err
}

func (s *Store) GetBlocks(symbol string, limit int) ([]Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT id, coin_symbol, height,
		COALESCE(block_hash,''), COALESCE(block_time,''),
		COALESCE(miner_address,''), COALESCE(worker_name,''), COALESCE(share_difficulty,0), COALESCE(reward,0), shares_since_last, luck_percent, created_at
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
			&b.LuckPercent, &b.CreatedAt); err == nil {
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	s.db.Exec(`INSERT INTO worker_best_diff (coin_symbol, worker_name, best_all_time, updated_at, last_difficulty)
		VALUES (?, ?, 0, ?, ?)
		ON CONFLICT(coin_symbol, worker_name) DO UPDATE SET last_difficulty=excluded.last_difficulty, updated_at=excluded.updated_at`,
		symbol, workerName, now, difficulty)
}

func (s *Store) GetWorkerLastDifficulty(symbol, workerName string) float64 {
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

func (s *Store) Close() error { return s.db.Close() }
