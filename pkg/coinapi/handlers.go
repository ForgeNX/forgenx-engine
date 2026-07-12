package coinapi

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// CoinAPI handles HTTP requests for coin app endpoints.
type CoinAPI struct {
	store          *Store
	engineAPIURL   string // e.g. "http://localhost:8080"
	nodeRPCFunc    NodeRPCFunc
	coinConfigFunc  CoinConfigFunc
	logsFunc        LogsFunc
	donationFunc    DonationFunc
	portStatusFunc  PortStatusFunc
}

// NodeRPCFunc fetches node info for a coin symbol.
type NodeRPCFunc func(symbol string) map[string]interface{}

// CoinConfigFunc fetches coin settings for a coin symbol.
type CoinConfigFunc func(symbol string) map[string]interface{}

// LogsFunc fetches recent logs for a coin.
type LogsFunc func(symbol string, tail int) []string

// DonationFunc looks up the donation address for a coin symbol and network.
type DonationFunc func(symbol, network string) (string, error)

// PortStatusFunc returns whether V1 and V2 stratum ports are live for a coin symbol.
type PortStatusFunc func(symbol string) (v1, v2 bool)

func NewCoinAPI(store *Store, engineAPIURL string) *CoinAPI {
	return &CoinAPI{
		store:        store,
		engineAPIURL: engineAPIURL,
	}
}

func (c *CoinAPI) SetNodeRPCFunc(f NodeRPCFunc)       { c.nodeRPCFunc = f }
func (c *CoinAPI) SetCoinConfigFunc(f CoinConfigFunc) { c.coinConfigFunc = f }
func (c *CoinAPI) SetLogsFunc(f LogsFunc)             { c.logsFunc = f }
func (c *CoinAPI) SetDonationFunc(f DonationFunc)     { c.donationFunc = f }
func (c *CoinAPI) SetPortStatusFunc(f PortStatusFunc) { c.portStatusFunc = f }

// ── JSON helpers ──────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ── Engine API proxy ──────────────────────────────────────────────────────────

func (c *CoinAPI) fetchEngineJSON(path string) (map[string]interface{}, error) {
	resp, err := http.Get(c.engineAPIURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// ── /api/engine/stats ─────────────────────────────────────────────────────────

func (c *CoinAPI) HandleEngineStats(w http.ResponseWriter, r *http.Request) {
	data, err := c.fetchEngineJSON("/stats")
	if err != nil {
		writeError(w, 502, "engine unavailable")
		return
	}

	uptime := int64(0)
	if v, ok := data["uptime_seconds"].(float64); ok {
		uptime = int64(v)
	}

	coins, _ := data["coins"].(map[string]interface{})
	for symbol, coinDataRaw := range coins {
		coinData, ok := coinDataRaw.(map[string]interface{})
		if !ok {
			continue
		}
		current := int64(getFloat(coinData, "shares_accepted"))
		currentRej := int64(getFloat(coinData, "shares_rejected"))
		currentStale := int64(getFloat(coinData, "shares_stale"))

		last := c.store.GetLastCounters(symbol)
		offset := last.SharesOffset
		offsetRej := last.SharesRejectedOffset
		offsetStale := last.SharesStaleOffset
		prevUptime := last.Uptime

		restarted := uptime < prevUptime && last.SharesAccepted > 0
		if restarted {
			offset += last.SharesAccepted
			offsetRej += last.SharesRejected
			offsetStale += last.SharesStale
		}

		c.store.UpdateCounters(symbol, last.BlocksFound, current,
			offset, currentRej, offsetRej, currentStale, offsetStale, uptime)

		coinData["shares_accepted"] = current + offset
		coinData["shares_rejected"] = currentRej + offsetRej
		coinData["shares_stale"] = currentStale + offsetStale
	}

	writeJSON(w, data)
}

// ── /api/engine/miners ────────────────────────────────────────────────────────

func (c *CoinAPI) HandleEngineMiners(w http.ResponseWriter, r *http.Request) {
	data, err := c.fetchEngineJSON("/miners")
	if err != nil {
		writeError(w, 502, "engine unavailable")
		return
	}
	writeJSON(w, data)
}

// ── /api/apps/{coin}/workers ──────────────────────────────────────────────────

func (c *CoinAPI) HandleWorkers(w http.ResponseWriter, r *http.Request, symbol string) {
	minersData, err := c.fetchEngineJSON("/miners")
	if err != nil {
		writeJSON(w, map[string]interface{}{"workers": []interface{}{}})
		return
	}

	miners, _ := minersData["miners"].(map[string]interface{})
	coinMiners, _ := miners[symbol].([]interface{})

	workerInfos := c.store.GetWorkerBestDiffs(symbol)
	var workers []map[string]interface{}

	for _, mRaw := range coinMiners {
		m, ok := mRaw.(map[string]interface{})
		if !ok {
			continue
		}

		workerName := getString(m, "worker_name")
		sessionBest := getFloat(m, "best_difficulty_session")
		if sessionBest == 0 {
			sessionBest = getFloat(m, "best_difficulty")
		}
		allTimeBest := c.store.UpdateWorkerBestDiff(symbol, workerName, sessionBest)

		// Shares
		sharesAccepted := int64(getFloat(m, "shares_accepted"))
		sessionSharesAccepted := int64(getFloat(m, "session_shares_accepted"))
		if sessionSharesAccepted == 0 {
			sessionSharesAccepted = sharesAccepted
		}
		sessionSharesRejected := int64(getFloat(m, "session_shares_rejected"))
		if sessionSharesRejected == 0 {
			sessionSharesRejected = int64(getFloat(m, "shares_rejected"))
		}

		winfo := workerInfos[workerName]
		offset := winfo.SharesOffset
		invalidOffset := winfo.InvalidSharesOffset
		currentValid := sharesAccepted + offset
		currentInvalid := sessionSharesRejected + invalidOffset

		// Detect engine restart
		prevAlltime := c.store.GetWorkerSharesAlltime(symbol, workerName)
		restarted := prevAlltime.Valid > 0 && sharesAccepted < (prevAlltime.Valid-offset)
		if restarted {
			offset = prevAlltime.Valid
			invalidOffset = prevAlltime.Invalid
			c.store.SetWorkerSharesOffset(symbol, workerName, offset, invalidOffset)
			currentValid = sharesAccepted + offset
			currentInvalid = sessionSharesRejected + invalidOffset
		}

		// Snapshot
		c.store.RecordWorkerSnapshot(symbol, workerName, currentValid, currentInvalid,
			int64(getFloat(m, "shares_stale")))

		w48 := c.store.GetWorkerShares48hLive(symbol, workerName, currentValid, currentInvalid)
		alltime := c.store.GetWorkerSharesAlltime(symbol, workerName)

		// Parse name parts
		nameParts := strings.SplitN(workerName, ".", 2)
		payoutAddress := workerName
		if len(nameParts) == 2 {
			payoutAddress = nameParts[0]
		}

		remoteAddr := getString(m, "remote_addr")
		ip := remoteAddr
		if idx := strings.LastIndex(remoteAddr, ":"); idx >= 0 {
			ip = remoteAddr[:idx]
		}

		vendor := getString(m, "vendor")
		if vendor != "" {
			vendor = strings.ToUpper(vendor[:1]) + vendor[1:]
		}

		lastShare := getString(m, "last_share_time")
		if lastShare == "0001-01-01T00:00:00Z" {
			lastShare = ""
		}

		workers = append(workers, map[string]interface{}{
			"name":                workerName,
			"online":              true,
			"hashrate":            getFloat(m, "hashrate_5m"),
			"hashrate_15m":        getFloat(m, "hashrate_15m"),
			"hashrate_5m":         getFloat(m, "hashrate_5m"),
			"difficulty":          getFloat(m, "difficulty"),
			"connected_at":        getString(m, "connected_at"),
			"best_session":        sessionBest,
			"best_all_time":       allTimeBest,
			"last_share":          lastShare,
			"valid_shares":        sessionSharesAccepted,
			"invalid_shares":      sessionSharesRejected,
			"stale_shares":        int64(getFloat(m, "shares_stale")),
			"protocol":            getString(m, "protocol"),
			"shares_48h_valid":    w48.Valid,
			"shares_48h_invalid":  w48.Invalid,
			"shares_alltime_valid":   alltime.Valid,
			"shares_alltime_invalid": alltime.Invalid,
			"payout_address": payoutAddress,
			"ip":             ip,
			"device":         vendor,
		})
	}

	// ── Offline workers from historical data ────────────────────────────────
	onlineNames := make(map[string]bool)
	for _, w := range workers {
		if name, ok := w["name"].(string); ok {
			onlineNames[name] = true
		}
	}
	allKnown := c.store.GetWorkerBestDiffs(symbol)
	for workerName, info := range allKnown {
		if onlineNames[workerName] {
			continue
		}
		// Skip workers with no last_seen (freshly deleted or never connected)
		if info.LastSeen == "" {
			continue
		}
		// 48h shares using alltime as effective current (worker is offline)
		alltimeShares := c.store.GetWorkerSharesAlltime(symbol, workerName)
		w48 := c.store.GetWorkerShares48hLive(symbol, workerName, alltimeShares.Valid, alltimeShares.Invalid)

		// Compute last session duration
		lastSessionDuration := ""
		if info.LastSeen != "" && info.ConnectedAt != "" {
			if tDisc, err1 := time.Parse(time.RFC3339Nano, info.LastSeen); err1 == nil {
				if tConn, err2 := time.Parse(time.RFC3339Nano, info.ConnectedAt); err2 == nil {
					secs := int(tDisc.Sub(tConn).Seconds())
					if secs > 0 {
						switch {
						case secs < 60:
							lastSessionDuration = fmt.Sprintf("%ds", secs)
						case secs < 3600:
							lastSessionDuration = fmt.Sprintf("%dm %ds", secs/60, secs%60)
						case secs < 86400:
							lastSessionDuration = fmt.Sprintf("%dh %dm", secs/3600, (secs%3600)/60)
						default:
							lastSessionDuration = fmt.Sprintf("%dd %dh", secs/86400, (secs%86400)/3600)
						}
					}
				}
			}
		}

		workers = append(workers, map[string]interface{}{
			"name":                    workerName,
			"online":                  false,
			"hashrate":                0,
			"hashrate_15m":            0,
			"hashrate_5m":             0,
			"difficulty":              0,
			"connected_at":            nil,
			"best_session":            0,
			"best_all_time":           info.BestAllTime,
			"last_share":              nil,
			"last_seen":               info.LastSeen,
			"last_session_duration":   lastSessionDuration,
			"valid_shares":            0,
			"invalid_shares":          0,
			"stale_shares":            0,
			"shares_48h_valid":        w48.Valid,
			"shares_48h_invalid":      w48.Invalid,
			"shares_alltime_valid":    alltimeShares.Valid,
			"shares_alltime_invalid":  alltimeShares.Invalid,
			"protocol":                "v1",
		})
	}

	if workers == nil {
		workers = []map[string]interface{}{}
	}
	writeJSON(w, map[string]interface{}{"workers": workers})
}

// ── /api/apps/{coin}/worker-shares-48h ───────────────────────────────────────

func (c *CoinAPI) HandleWorkerShares48h(w http.ResponseWriter, r *http.Request, symbol string) {
	shares := c.store.GetWorkerShares48h(symbol)
	result := make(map[string]interface{})
	for name, counts := range shares {
		result[name] = map[string]interface{}{
			"valid":   counts.Valid,
			"invalid": counts.Invalid,
			"stale":   counts.Stale,
		}
	}
	writeJSON(w, map[string]interface{}{"workers": result})
}

// ── /api/apps/{coin}/status ───────────────────────────────────────────────────

func (c *CoinAPI) HandleStatus(w http.ResponseWriter, r *http.Request, symbol string) {
	statsData, _ := c.fetchEngineJSON("/stats")
	minersData, _ := c.fetchEngineJSON("/miners")

	coinStats := map[string]interface{}{}
	if statsData != nil {
		if coins, ok := statsData["coins"].(map[string]interface{}); ok {
			if cs, ok := coins[symbol].(map[string]interface{}); ok {
				coinStats = cs
			}
		}
	}

	// Apply persistence offsets
	persisted := c.store.GetLastCounters(symbol)
	persistedAccepted := persisted.SharesAccepted + persisted.SharesOffset
	persistedRejected := persisted.SharesRejected + persisted.SharesRejectedOffset
	persistedStale := persisted.SharesStale + persisted.SharesStaleOffset

	sharesAccepted := max64(persistedAccepted, int64(getFloat(coinStats, "shares_accepted")))
	sharesRejected := max64(persistedRejected, int64(getFloat(coinStats, "shares_rejected")))
	sharesStale := max64(persistedStale, int64(getFloat(coinStats, "shares_stale")))

	// Pool stats from miners
	bestSessionDiff := 0.0
	lastShareTime := ""
	workerCount := 0
	totalHashrate := 0.0

	if minersData != nil {
		if miners, ok := minersData["miners"].(map[string]interface{}); ok {
			if coinMiners, ok := miners[symbol].([]interface{}); ok {
				workerCount = len(coinMiners)
				for _, mRaw := range coinMiners {
					m, ok := mRaw.(map[string]interface{})
					if !ok {
						continue
					}
					if bd := getFloat(m, "best_difficulty"); bd > bestSessionDiff {
						bestSessionDiff = bd
					}
					hr := getFloat(m, "hashrate_5m")
					if hr == 0 {
						hr = getFloat(m, "hashrate_1m")
					}
					totalHashrate += hr
					lst := getString(m, "last_share_time")
					if lst != "" && !strings.HasPrefix(lst, "0001") {
						if lastShareTime == "" || lst > lastShareTime {
							lastShareTime = lst
						}
					}
				}
			}
		}
	}

	uptime := int64(0)
	if statsData != nil {
		uptime = int64(getFloat(statsData, "uptime_seconds"))
	}

	nodeInfo := map[string]interface{}{}
	engineConnected := false
	if c.nodeRPCFunc != nil {
		nodeInfo = c.nodeRPCFunc(symbol)
		if v, ok := nodeInfo["connected"].(bool); ok {
			engineConnected = v
			delete(nodeInfo, "connected")
		}
	}
	writeJSON(w, map[string]interface{}{
		"engine_connected":  engineConnected,
		"zmq_connected":     engineConnected,
		"stratum_v1_open":   func() bool { if c.portStatusFunc != nil { v1, _ := c.portStatusFunc(symbol); return v1 }; return engineConnected }(),
		"stratum_v2_open":   func() bool { if c.portStatusFunc != nil { _, v2 := c.portStatusFunc(symbol); return v2 }; return false }(),
		"pool": map[string]interface{}{
			"shares_accepted":   sharesAccepted,
			"shares_rejected":   sharesRejected,
			"shares_stale":      sharesStale,
			"blocks_found":      persisted.BlocksFound,
			"last_block_time":   getString(coinStats, "last_block_time"),
			"uptime_seconds":    uptime,
			"best_session_diff": bestSessionDiff,
			"last_share_time":   lastShareTime,
			"hashrate":          totalHashrate,
			"max_hashrate":      c.store.GetMaxPoolHashrate(symbol),
			"worker_count":      workerCount,
		},
		"node": nodeInfo,
	})
}

// ── /api/apps/{coin}/worker DELETE ───────────────────────────────────────────

func (c *CoinAPI) HandleDeleteWorker(w http.ResponseWriter, r *http.Request, symbol, workerName string) {
	if r.Method != http.MethodDelete {
		writeError(w, 405, "method not allowed")
		return
	}
	c.store.DeleteWorker(symbol, workerName)
	writeJSON(w, map[string]interface{}{"success": true})
}

// ── Background: snapshot thread ───────────────────────────────────────────────

// StartSnapshotThread runs the 60-second worker share snapshot background task.
func (c *CoinAPI) StartSnapshotThread() {
	go func() {
		// Initial delay so engine has time to start
		time.Sleep(10 * time.Second)
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			c.runSnapshot()
			c.runHistorySnapshot()
		}
	}()
}

func (c *CoinAPI) runSnapshot() {
	minersData, err := c.fetchEngineJSON("/miners")
	if err != nil {
		return
	}
	miners, _ := minersData["miners"].(map[string]interface{})
	for symbol, coinMinersRaw := range miners {
		coinMiners, _ := coinMinersRaw.([]interface{})
		workerInfos := c.store.GetWorkerBestDiffs(symbol)
		for _, mRaw := range coinMiners {
			m, ok := mRaw.(map[string]interface{})
			if !ok {
				continue
			}
			name := getString(m, "worker_name")
			rawValid := int64(getFloat(m, "shares_accepted"))
			rawInvalid := int64(getFloat(m, "session_shares_rejected"))
			if rawInvalid == 0 {
				rawInvalid = int64(getFloat(m, "shares_rejected"))
			}
			stale := int64(getFloat(m, "shares_stale"))

			winfo := workerInfos[name]
			snapOffset := winfo.SharesOffset
			snapInvalidOffset := winfo.InvalidSharesOffset

			// Detect restart
			prevAlltime := c.store.GetWorkerSharesAlltime(symbol, name)
			restarted := prevAlltime.Valid > 0 && rawValid < (prevAlltime.Valid-snapOffset)
			if restarted {
				snapOffset = prevAlltime.Valid
				snapInvalidOffset = prevAlltime.Invalid
				c.store.SetWorkerSharesOffset(symbol, name, snapOffset, snapInvalidOffset)
			}

			c.store.RecordWorkerSnapshot(symbol, name,
				rawValid+snapOffset, rawInvalid+snapInvalidOffset, stale)
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func getFloat(m map[string]interface{}, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func symFromCoinID(coinID string) string {
	// forgebch -> BCH, forgebtc -> BTC, etc.
	id := strings.TrimPrefix(strings.ToLower(coinID), "forge")
	return strings.ToUpper(id)
}

// RegisterRoutes adds all coin API routes to the given mux.
func (c *CoinAPI) RegisterRoutes(mux *http.ServeMux) {
	// Engine proxy
	mux.HandleFunc("/api/engine/stats", c.HandleEngineStats)
	mux.HandleFunc("/api/engine/miners", c.HandleEngineMiners)
	mux.HandleFunc("/api/engine/donation-address/", c.HandleDonationAddress)
	mux.HandleFunc("/api/engine/logs", c.HandleEngineLogs)

	// Coin app routes — matched by prefix, coin ID extracted from path
	mux.HandleFunc("/api/apps/", func(w http.ResponseWriter, r *http.Request) {
		// Path: /api/apps/{coinID}/{endpoint}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/apps/"), "/", 2)
		if len(parts) < 2 {
			http.NotFound(w, r)
			return
		}
		coinID := parts[0]
		endpoint := parts[1]
		symbol := symFromCoinID(coinID)

		switch {
		case endpoint == "workers" && r.Method == http.MethodGet:
			c.HandleWorkers(w, r, symbol)
		case strings.HasPrefix(endpoint, "workers/") && r.Method == http.MethodDelete:
			workerName := strings.TrimPrefix(endpoint, "workers/")
			c.HandleDeleteWorker(w, r, symbol, workerName)
		case endpoint == "worker-shares-48h":
			c.HandleWorkerShares48h(w, r, symbol)
		case endpoint == "status":
			c.HandleStatus(w, r, symbol)
		case endpoint == "node":
			c.HandleNode(w, r, symbol)
		case endpoint == "pool":
			c.HandlePool(w, r, symbol)
		case endpoint == "blocks":
			c.HandleBlocks(w, r, symbol)
		case endpoint == "history":
			c.HandleHistory(w, r, symbol)
		case endpoint == "logs":
			c.HandleLogs(w, r, coinID)
		case endpoint == "settings" && r.Method == http.MethodGet:
			c.HandleSettingsGet(w, r, coinID)
		case endpoint == "settings" && r.Method == http.MethodPost:
			c.HandleSettingsPost(w, r, coinID)
		case endpoint == "rpc-credentials" && r.Method == http.MethodGet:
			c.HandleRPCCredentialsGet(w, r, coinID)
		case endpoint == "rpc-credentials" && r.Method == http.MethodPost:
			c.HandleRPCCredentialsPost(w, r, coinID)
		case (endpoint == "start" || endpoint == "stop" || endpoint == "restart") && r.Method == http.MethodPost:
			c.HandleAction(w, r, coinID, endpoint)
		default:
			http.NotFound(w, r)
		}
	})
}

// ── /api/apps/{coin}/node ─────────────────────────────────────────────────────

func (c *CoinAPI) HandleNode(w http.ResponseWriter, r *http.Request, symbol string) {
	if c.nodeRPCFunc == nil {
		writeError(w, 503, "node RPC not available")
		return
	}
	info := c.nodeRPCFunc(symbol)
	delete(info, "connected")
	writeJSON(w, info)
}

// ── /api/apps/{coin}/pool ─────────────────────────────────────────────────────

func (c *CoinAPI) HandlePool(w http.ResponseWriter, r *http.Request, symbol string) {
	statsData, _ := c.fetchEngineJSON("/stats")

	coinStats := map[string]interface{}{}
	if statsData != nil {
		if coins, ok := statsData["coins"].(map[string]interface{}); ok {
			if cs, ok := coins[symbol].(map[string]interface{}); ok {
				coinStats = cs
			}
		}
	}

	persisted := c.store.GetLastCounters(symbol)
	persistentAccepted := persisted.SharesAccepted + persisted.SharesOffset

	engineConnected := statsData != nil
	hashrate := 0.0
	workers := 0

	minersData, _ := c.fetchEngineJSON("/miners")
	if minersData != nil {
		if miners, ok := minersData["miners"].(map[string]interface{}); ok {
			if coinMiners, ok := miners[symbol].([]interface{}); ok {
				workers = len(coinMiners)
				for _, mRaw := range coinMiners {
					if m, ok := mRaw.(map[string]interface{}); ok {
						hashrate += getFloat(m, "hashrate_5m")
					}
				}
			}
		}
	}

	syncProgress := 0.0
	if c.nodeRPCFunc != nil {
		if info := c.nodeRPCFunc(symbol); info != nil {
			if v, ok := info["sync_pct"].(float64); ok {
				syncProgress = v / 100.0
			}
		}
	}

	uptime := int64(0)
	if statsData != nil {
		uptime = int64(getFloat(statsData, "uptime_seconds"))
	}

	writeJSON(w, map[string]interface{}{
		"connected":         engineConnected,
		"hashrate":          hashrate,
		"max_pool_hashrate": 0,
		"symbol":            symbol,
		"sync_progress":     syncProgress,
		"workers":           workers,
		"shares_accepted":   max64(int64(persistentAccepted), int64(getFloat(coinStats, "shares_accepted"))),
		"shares_rejected":   int64(getFloat(coinStats, "shares_rejected")),
		"shares_stale":      int64(getFloat(coinStats, "shares_stale")),
		"blocks_found":      int64(getFloat(coinStats, "blocks_found")),
		"last_block_time":   getString(coinStats, "last_block_time"),
		"uptime_seconds":    uptime,
	})
}

// ── /api/apps/{coin}/blocks ───────────────────────────────────────────────────

func (c *CoinAPI) HandleBlocks(w http.ResponseWriter, r *http.Request, symbol string) {
	limitStr := r.URL.Query().Get("limit")
	limit := 20
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			limit = n
		}
	}
	blocks, err := c.store.GetBlocks(symbol, limit)
	if err != nil {
		writeError(w, 500, "failed to query blocks")
		return
	}
	luck := c.store.GetLuckStats(symbol)
	total := c.store.GetBlockCount(symbol)

	var out []map[string]interface{}
	for _, b := range blocks {
		out = append(out, map[string]interface{}{
			"id":                b.ID,
			"coin_symbol":       b.CoinSymbol,
			"height":            b.Height,
			"block_hash":        b.BlockHash,
			"block_time":        b.BlockTime,
			"miner_address":     b.MinerAddress,
			"shares_since_last": b.SharesSinceLast,
			"luck_percent":      b.LuckPercent,
			"created_at":        b.CreatedAt,
		})
	}
	if out == nil {
		out = []map[string]interface{}{}
	}
	writeJSON(w, map[string]interface{}{
		"blocks": out,
		"luck_stats": map[string]interface{}{
			"avg_luck":   luck.AvgLuck,
			"luckiest":   luck.Luckiest,
			"unluckiest": luck.Unluckiest,
		},
		"total": total,
	})
}

// ── /api/apps/{coin}/logs ─────────────────────────────────────────────────────

// restartContainer restarts a Docker container via the Docker socket API.
func restartContainer(container string) error {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	url := fmt.Sprintf("http://localhost/containers/%s/restart?t=10", container)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("restart request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("docker socket: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return fmt.Errorf("docker restart returned %d", resp.StatusCode)
	}
	return nil
}

// HandleEngineLogs fetches logs for the engine container itself.
func (c *CoinAPI) HandleEngineLogs(w http.ResponseWriter, r *http.Request) {
	tailStr := r.URL.Query().Get("tail")
	tail := 100
	if tailStr != "" {
		if n, err := strconv.Atoi(tailStr); err == nil && n > 0 {
			tail = n
		}
	}
	// Try common engine container names
	containerNames := []string{"forgenx-engine-engine-1", "forgenx-engine_engine_1", "forgenx-engine"}
	var output string
	var err error
	for _, name := range containerNames {
		output, err = dockerLogs(name, tail)
		if err == nil {
			break
		}
	}
	if err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "logs": "", "error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{"success": true, "logs": output})
}

// HandleLogs fetches container logs via the Docker socket API directly.
// This avoids any dependency on the docker CLI binary, making the engine
// self-contained and deployable on Umbrel OS and other platforms where the
// CLI is not available inside the container.
func (c *CoinAPI) HandleLogs(w http.ResponseWriter, r *http.Request, coinID string) {
	tailStr := r.URL.Query().Get("tail")
	tail := 100
	if tailStr != "" {
		if n, err := strconv.Atoi(tailStr); err == nil && n > 0 {
			tail = n
		}
	}
	if tail > 5000 {
		tail = 5000
	}

	// Derive container name: forgebch -> forgebch-node
	container := coinID + "-node"

	output, err := dockerLogs(container, tail)
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"success": false,
			"logs":    "",
			"error":   err.Error(),
		})
		return
	}

	writeJSON(w, map[string]interface{}{
		"success": true,
		"logs":    output,
	})
}

// dockerLogs fetches the last `tail` lines of logs for a container by calling
// the Docker daemon REST API over /var/run/docker.sock. No docker CLI needed.
func dockerLogs(container string, tail int) (string, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}

	// Docker API: GET /containers/{name}/logs?stdout=1&stderr=1&tail=N
	url := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=1&stderr=1&tail=%d",
		container, tail)
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("docker socket: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", fmt.Errorf("container %q not found", container)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("docker API returned %d", resp.StatusCode)
	}

	// Docker multiplexes stdout/stderr with an 8-byte header per frame:
	// [stream_type(1), 0, 0, 0, size(4 big-endian)] followed by `size` bytes of log data.
	// We strip the headers and collect the raw log text.
	var out strings.Builder
	buf := make([]byte, 8)
	for {
		_, err := io.ReadFull(resp.Body, buf)
		if err != nil {
			break // EOF or error — done
		}
		size := int(buf[4])<<24 | int(buf[5])<<16 | int(buf[6])<<8 | int(buf[7])
		if size == 0 {
			continue
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, payload); err != nil {
			break
		}
		out.Write(payload)
	}

	return strings.TrimRight(out.String(), "\n"), nil
}

// ── /api/apps/{coin}/settings GET ────────────────────────────────────────────

func (c *CoinAPI) HandleSettingsGet(w http.ResponseWriter, r *http.Request, coinID string) {
	symbol := strings.ToLower(strings.TrimPrefix(coinID, "forge"))
	prefix := strings.ToUpper(symbol) + "_"

	envPath := "/opt/forgenx/apps/" + coinID + "/.env"
	configPath := "/pool/coins/" + symbol + ".json"
	manifestPath := "/opt/forgenx/apps/" + coinID + "/umbrel-app.yml"

	env := readEnvFile(envPath)
	coinCfg := readJSONFile(configPath)

	// Parse version/releaseDate from YAML manifest (simple line scan, no YAML dep)
	appVersion := "1.0.0"
	releaseDate := ""
	if manifestData, err := os.ReadFile(manifestPath); err == nil {
		for _, line := range strings.Split(string(manifestData), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "version:") {
				appVersion = strings.Trim(strings.TrimPrefix(line, "version:"), " \"")
			}
			if strings.HasPrefix(line, "releaseDate:") {
				releaseDate = strings.Trim(strings.TrimPrefix(line, "releaseDate:"), " \"")
			}
		}
	}

	vardiff := getNestedMap(coinCfg, "vardiff")
	stratum := getNestedMap(coinCfg, "stratum")
	node    := getNestedMap(coinCfg, "node")

	// Extract ZMQ port from tcp://host:port
	zmqHashblock := 28333
	if zmqURL := getNestedStr(node, "zmq_hashblock"); zmqURL != "" {
		if idx := strings.LastIndex(zmqURL, ":"); idx >= 0 {
			if port, err := strconv.Atoi(zmqURL[idx+1:]); err == nil {
				zmqHashblock = port
			}
		}
	}

	pruneSizeMb := envInt(env, prefix+"PRUNE", 550)

	writeJSON(w, map[string]interface{}{
		"appVersion":         appVersion,
		"releaseDate":        releaseDate,
		"network":            envStr(env, prefix+"NETWORK", "mainnet"),
		"prune":              envStr(env, prefix+"PRUNE", "550") != "0",
		"prune_size_mb":      pruneSizeMb,
		"pruneSize":          pruneSizeMb,
		"rpc_user":           envStr(env, prefix+"RPC_USER", "forgenx"),
		"stratum_port":       getNestedInt(stratum, "port", 3334),
		"payoutAddress":      envStr(env, prefix+"PAYOUT_ADDRESS", ""),
		"workerName":         envStr(env, prefix+"WORKER_NAME", ""),
		"targetTime":         getNestedFloat(vardiff, "target_time", 45),
		"retargetTime":       getNestedFloat(vardiff, "retarget_time", 300),
		"variancePercent":    getNestedFloat(vardiff, "variance_percent", 30),
		"onNewBlock":         getNestedBool(vardiff, "on_new_block", true),
		"pingEnabled":        getNestedBool(stratum, "ping_enabled", true),
		"pingInterval":       getNestedInt(stratum, "ping_interval", 30),
		"staleShareGrace":    getNestedInt(stratum, "stale_share_grace", 5),
		"lowDiffShareGrace":  getNestedInt(stratum, "low_diff_share_grace", 5),
		"zmqEnabled":         getNestedBool(node, "zmq_enabled", true),
		"acceptSuggestDiff":  getNestedBool(stratum, "accept_suggest_diff", false),
		"zmqHashblock":       zmqHashblock,
		"templateRefresh":    getNestedInt(coinCfg, "template_refresh_interval", 100),
		"diffPreset":         envStr(env, prefix+"DIFF_PRESET", "home"),
		"startDiff":          envInt(env, prefix+"START_DIFF", 128),
		"minDiff":            envInt(env, prefix+"MIN_DIFF", 32),
		"maxDiff":            envInt(env, prefix+"MAX_DIFF", 4096),
		"autoStart":          envStr(env, prefix+"AUTO_START", "true") == "true",
		"donation1Addr":      envStr(env, prefix+"DONATION1_ADDR", ""),
		"donation1Pct":       envFloat(env, prefix+"DONATION1_PCT", 1.0),
		"donation2Addr":      envStr(env, prefix+"DONATION2_ADDR", ""),
		"donation2Pct":       envFloat(env, prefix+"DONATION2_PCT", 0.0),
		"configVersion":      getStr(coinCfg, "configVersion", "1.0"),
		"sv2Enabled":         getNestedBool(stratum, "sv2_enabled", false),
		"sv2Port":            getNestedInt(stratum, "sv2_port", 4334),
		"sv2AuthorityPubkey": getNestedStr(stratum, "sv2_authority_pubkey"),
		"connectionTimeout":  getNestedInt(stratum, "connection_timeout", 600),
	})
}

// ── /api/apps/{coin}/rpc-credentials GET ─────────────────────────────────────

func (c *CoinAPI) HandleRPCCredentialsGet(w http.ResponseWriter, r *http.Request, coinID string) {
	symbol := strings.ToUpper(strings.TrimPrefix(coinID, "forge"))
	prefix := symbol + "_"
	envPath := "/opt/forgenx/apps/" + coinID + "/.env"
	env := readEnvFile(envPath)
	if len(env) == 0 {
		writeError(w, 404, "could not read settings for "+coinID)
		return
	}
	writeJSON(w, map[string]interface{}{
		"rpc_user": envStr(env, prefix+"RPC_USER", "forgenx"),
		"rpc_pass": envStr(env, prefix+"RPC_PASS", ""),
	})
}

// ── File/env helpers ──────────────────────────────────────────────────────────

func readEnvFile(path string) map[string]string {
	env := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return env
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}
		env[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
	}
	return env
}

func readJSONFile(path string) map[string]interface{} {
	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]interface{}{}
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return map[string]interface{}{}
	}
	return result
}

func getNestedMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return map[string]interface{}{}
}

func getNestedStr(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getStr(m map[string]interface{}, key, def string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return def
}

func getNestedFloat(m map[string]interface{}, key string, def float64) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return def
}

func getNestedInt(m map[string]interface{}, key string, def int) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	return def
}

func getNestedBool(m map[string]interface{}, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return def
}

func envStr(env map[string]string, key, def string) string {
	if v, ok := env[key]; ok && v != "" {
		return v
	}
	return def
}

func envInt(env map[string]string, key string, def int) int {
	if v, ok := env[key]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(env map[string]string, key string, def float64) float64 {
	if v, ok := env[key]; ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// ── /api/apps/{coin}/settings POST ───────────────────────────────────────────

func (c *CoinAPI) HandleSettingsPost(w http.ResponseWriter, r *http.Request, coinID string) {
	symbol := strings.ToLower(strings.TrimPrefix(coinID, "forge"))
	prefix := strings.ToUpper(symbol) + "_"
	envPath := "/opt/forgenx/apps/" + coinID + "/.env"
	configPath := "/pool/coins/" + symbol + ".json"

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid request body")
		return
	}

	// ── 1. Update .env ────────────────────────────────────────────────────────
	env := readEnvFile(envPath)

	if v, ok := body["network"].(string); ok {
		env[prefix+"NETWORK"] = v
	}
	if v, ok := body["pruneSize"].(float64); ok {
		mb := int(v)
		if mb < 550 {
			mb = 550
		}
		env[prefix+"PRUNE"] = strconv.Itoa(mb)
	}
	if v, ok := body["prune"].(bool); ok && !v {
		env[prefix+"PRUNE"] = "0"
	}
	if v, ok := body["autoStart"].(bool); ok {
		if v {
			env[prefix+"AUTO_START"] = "true"
		} else {
			env[prefix+"AUTO_START"] = "false"
		}
	}
	if v, ok := body["payoutAddress"].(string); ok {
		env[prefix+"PAYOUT_ADDRESS"] = v
	}
	if v, ok := body["workerName"].(string); ok {
		env[prefix+"WORKER_NAME"] = v
	}
	if v, ok := body["diffPreset"].(string); ok {
		env[prefix+"DIFF_PRESET"] = v
	}
	if v, ok := body["startDiff"].(float64); ok {
		env[prefix+"START_DIFF"] = strconv.Itoa(int(v))
	}
	if v, ok := body["minDiff"].(float64); ok {
		env[prefix+"MIN_DIFF"] = strconv.Itoa(int(v))
	}
	if v, ok := body["maxDiff"].(float64); ok {
		env[prefix+"MAX_DIFF"] = strconv.Itoa(int(v))
	}
	if v, ok := body["donation1Addr"].(string); ok {
		env[prefix+"DONATION1_ADDR"] = v
	}
	if v, ok := body["donation1Pct"].(float64); ok {
		env[prefix+"DONATION1_PCT"] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	if v, ok := body["donation2Addr"].(string); ok {
		env[prefix+"DONATION2_ADDR"] = v
	}
	if v, ok := body["donation2Pct"].(float64); ok {
		env[prefix+"DONATION2_PCT"] = strconv.FormatFloat(v, 'f', -1, 64)
	}

	if err := writeEnvFile(envPath, env); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}

	// ── 2. Update coin JSON config ────────────────────────────────────────────
	coinCfg := readJSONFile(configPath)
	if coinCfg == nil {
		coinCfg = map[string]interface{}{}
	}

	mining := getNestedMap(coinCfg, "mining")
	stratum := getNestedMap(coinCfg, "stratum")
	vardiff := getNestedMap(coinCfg, "vardiff")
	node := getNestedMap(coinCfg, "node")

	if v, ok := body["payoutAddress"].(string); ok {
		mining["address"] = v
	}
	if v, ok := body["network"].(string); ok {
		mining["network"] = v
	}
	if v, ok := body["startDiff"].(float64); ok {
		stratum["difficulty"] = int(v)
	}
	if v, ok := body["minDiff"].(float64); ok {
		vardiff["min_diff"] = int(v)
	}
	if v, ok := body["maxDiff"].(float64); ok {
		vardiff["max_diff"] = int(v)
	}
	if v, ok := body["targetTime"].(float64); ok {
		vardiff["target_time"] = int(v)
	}
	if v, ok := body["retargetTime"].(float64); ok {
		vardiff["retarget_time"] = int(v)
	}
	if v, ok := body["variancePercent"].(float64); ok {
		vardiff["variance_percent"] = int(v)
	}
	if v, ok := body["onNewBlock"].(bool); ok {
		vardiff["on_new_block"] = v
	vardiff["enabled"] = true
	}
	if v, ok := body["pingEnabled"].(bool); ok {
		stratum["ping_enabled"] = v
	}
	if v, ok := body["pingInterval"].(float64); ok {
		stratum["ping_interval"] = int(v)
	}
	if v, ok := body["staleShareGrace"].(float64); ok {
		stratum["stale_share_grace"] = int(v)
	}
	if v, ok := body["lowDiffShareGrace"].(float64); ok {
		stratum["low_diff_share_grace"] = int(v)
	}
	if v, ok := body["acceptSuggestDiff"].(bool); ok {
		stratum["accept_suggest_diff"] = v
	}
	if v, ok := body["zmqEnabled"].(bool); ok {
		node["zmq_enabled"] = v
	}
	if v, ok := body["zmqHashblock"].(float64); ok {
		node["zmq_hashblock"] = fmt.Sprintf("tcp://%s-node:%d", coinID, int(v))
	}
	if v, ok := body["templateRefresh"].(float64); ok {
		coinCfg["template_refresh_interval"] = int(v)
	}
	if v, ok := body["sv2Enabled"].(bool); ok {
		stratum["sv2_enabled"] = v
	}
	if v, ok := body["sv2Port"].(float64); ok {
		stratum["sv2_port"] = int(v)
	}
	if v, ok := body["connectionTimeout"].(float64); ok {
		stratum["connection_timeout"] = int(v)
	}
	if v, ok := body["donation1Pct"].(float64); ok {
		coinCfg["donation"] = map[string]interface{}{
			"enabled": v > 0,
			"percent": v,
		}
	}

	coinCfg["mining"] = mining
	coinCfg["stratum"] = stratum
	coinCfg["vardiff"] = vardiff
	coinCfg["node"] = node

	if err := writeJSONFile(configPath, coinCfg); err != nil {
		// .env already saved — log but don't fail
		writeJSON(w, map[string]interface{}{"success": true, "warning": "settings saved but config update failed: " + err.Error()})
		return
	}

	// Check if node settings changed (prune, network) — restart node container if so
	_, pruneChanged   := body["prune"]
	_, pruneSzChanged := body["pruneSize"]
	_, networkChanged := body["network"]
	needsNodeRestart := pruneChanged || pruneSzChanged || networkChanged
	if needsNodeRestart {
		go func() { restartContainer(coinID + "-node") }()
	}
	writeJSON(w, map[string]interface{}{"success": true, "nodeRestart": needsNodeRestart})
}

// ── /api/apps/{coin}/rpc-credentials POST ────────────────────────────────────

func (c *CoinAPI) HandleRPCCredentialsPost(w http.ResponseWriter, r *http.Request, coinID string) {
	symbol := strings.ToLower(strings.TrimPrefix(coinID, "forge"))
	prefix := strings.ToUpper(symbol) + "_"
	envPath := "/opt/forgenx/apps/" + coinID + "/.env"
	configPath := "/pool/coins/" + symbol + ".json"
	appDir := "/opt/forgenx/apps/" + coinID

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid request body")
		return
	}

	rpcUser := strings.TrimSpace(getString(body, "rpc_user"))
	rpcPass := strings.TrimSpace(getString(body, "rpc_pass"))
	if rpcUser == "" || rpcPass == "" {
		writeError(w, 400, "rpc_user and rpc_pass are required")
		return
	}

	// ── 1. Update .env (preserve existing lines, replace RPC fields) ──────────
	data, _ := os.ReadFile(envPath)
	var envLines []string
	for _, line := range strings.Split(string(data), "\n") {
		stripped := strings.TrimSpace(line)
		if strings.HasPrefix(stripped, prefix+"RPC_USER=") ||
			strings.HasPrefix(stripped, prefix+"RPC_PASS=") {
			continue
		}
		if stripped != "" || len(envLines) == 0 {
			envLines = append(envLines, line)
		}
	}
	envLines = append(envLines,
		prefix+"RPC_USER="+rpcUser,
		prefix+"RPC_PASS="+rpcPass,
	)
	if err := os.WriteFile(envPath, []byte(strings.Join(envLines, "\n")+"\n"), 0644); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "node_verified": false,
			"message": "Failed to update .env: " + err.Error()})
		return
	}

	// ── 2. Update coin JSON config ────────────────────────────────────────────
	coinCfg := readJSONFile(configPath)
	node := getNestedMap(coinCfg, "node")
	node["username"] = rpcUser
	node["password"] = rpcPass
	coinCfg["node"] = node
	if err := writeJSONFile(configPath, coinCfg); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "node_verified": false,
			"message": "Failed to update coin config: " + err.Error()})
		return
	}

	// ── 3. Restart node container via Docker socket API ───────────────────────
	// compose 'restart' does NOT re-read .env — must use 'up -d' equivalent.
	// We call the Docker API directly: stop then start the container, which
	// forces the daemon to re-interpolate .env on start.
	containerName := coinID + "-node"
	composeFile := appDir + "/docker-compose.yml"
	_ = composeFile // Docker socket approach below; compose file kept for reference

	if err := dockerRestartContainer(containerName); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "node_verified": false,
			"message": "Credentials saved but node restart failed: " + err.Error()})
		return
	}

	// ── 4. Poll until node RPC comes back online (max 60s) ───────────────────
	if c.nodeRPCFunc != nil {
		for i := 0; i < 20; i++ {
			time.Sleep(3 * time.Second)
			info := c.nodeRPCFunc(strings.ToUpper(symbol))
			if s, ok := info["status"].(string); ok && s == "online" {
				writeJSON(w, map[string]interface{}{
					"success":       true,
					"node_verified": true,
					"message":       "RPC credentials updated. Node verified online with new credentials.",
				})
				return
			}
		}
	}

	writeJSON(w, map[string]interface{}{
		"success":       true,
		"node_verified": false,
		"message":       "Credentials saved and node restarted, but could not verify it came back online within 60s. Check the Node tab.",
	})
}

// dockerRestartContainer stops and starts a container via the Docker socket API.
// This forces .env re-interpolation on start, unlike a simple 'restart'.
func dockerRestartContainer(container string) error {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	// Stop the container (timeout=10s for graceful shutdown)
	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=10", container)
	req, _ := http.NewRequest("POST", stopURL, nil)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("stop request failed: %w", err)
	}
	resp.Body.Close()
	// 204 = stopped, 304 = already stopped — both acceptable
	if resp.StatusCode != 204 && resp.StatusCode != 304 {
		return fmt.Errorf("stop returned %d", resp.StatusCode)
	}

	// Start the container
	startURL := fmt.Sprintf("http://localhost/containers/%s/start", container)
	req, _ = http.NewRequest("POST", startURL, bytes.NewReader(nil))
	resp, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("start request failed: %w", err)
	}
	resp.Body.Close()
	// 204 = started, 304 = already running
	if resp.StatusCode != 204 && resp.StatusCode != 304 {
		return fmt.Errorf("start returned %d", resp.StatusCode)
	}

	return nil
}

// writeEnvFile writes a KEY=VALUE map to a .env file.
func writeEnvFile(path string, env map[string]string) error {
	var lines []string
	lines = append(lines, "# ForgeBCH app environment")
	for k, v := range env {
		lines = append(lines, k+"="+v)
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644)
}

// writeJSONFile writes a map as indented JSON to a file.
func writeJSONFile(path string, v map[string]interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// ── /api/apps/{coin}/history ──────────────────────────────────────────────────

func (c *CoinAPI) HandleHistory(w http.ResponseWriter, r *http.Request, symbol string) {
	metric := r.URL.Query().Get("metric")
	trail := r.URL.Query().Get("trail")

	validMetrics := map[string]bool{
		"pool_hashrate_raw": true, "network_hashrate_raw": true, "difficulty": true,
	}
	if !validMetrics[metric] {
		writeError(w, 400, "invalid metric")
		return
	}

	t, ok := historyTrails[trail]
	if !ok {
		t = historyTrails["6h"]
		trail = "6h"
	}

	data := c.store.GetHistory(symbol, t.Seconds, t.Points, metric)
	writeJSON(w, map[string]interface{}{
		"metric": metric,
		"trail":  trail,
		"data":   data,
	})
}

// runHistorySnapshot records a metric sample for all active coins.
func (c *CoinAPI) runHistorySnapshot() {
	statsData, err := c.fetchEngineJSON("/stats")
	if err != nil {
		return
	}
	coins, _ := statsData["coins"].(map[string]interface{})
	for symbol := range coins {
		poolHashrate := 0.0
		networkHashrate := 0.0
		difficulty := 0.0

		minersData, _ := c.fetchEngineJSON("/miners")
		if minersData != nil {
			if miners, ok := minersData["miners"].(map[string]interface{}); ok {
				if coinMiners, ok := miners[symbol].([]interface{}); ok {
					for _, mRaw := range coinMiners {
						if m, ok := mRaw.(map[string]interface{}); ok {
							poolHashrate += getFloat(m, "hashrate_5m")
						}
					}
				}
			}
		}

		if c.nodeRPCFunc != nil {
			info := c.nodeRPCFunc(symbol)
			networkHashrate = getFloat(info, "network_hashrate_raw")
			difficulty = getFloat(info, "difficulty")
		}

		c.store.RecordSample(symbol, poolHashrate, networkHashrate, difficulty)
	}
}

// ── /api/engine/donation-address/{symbol} ────────────────────────────────────

func (c *CoinAPI) HandleDonationAddress(w http.ResponseWriter, r *http.Request) {
	// Path: /api/engine/donation-address/{symbol}
	symbol := strings.TrimPrefix(r.URL.Path, "/api/engine/donation-address/")
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		writeError(w, 400, "symbol required")
		return
	}
	network := r.URL.Query().Get("network")
	if network == "" {
		network = "mainnet"
	}
	if c.donationFunc == nil {
		writeError(w, 503, "donation address lookup not available")
		return
	}
	addr, err := c.donationFunc(symbol, network)
	if err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{
		"success": true,
		"symbol":  symbol,
		"network": network,
		"address": addr,
	})
}

// ── /api/apps/{coin}/{start|stop|restart} POST ───────────────────────────────

// HandleAction starts, stops, or restarts all containers in a coin app's
// Docker Compose project using the Docker socket API directly.
func (c *CoinAPI) HandleAction(w http.ResponseWriter, r *http.Request, coinID, action string) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	client := &http.Client{Transport: transport, Timeout: 120 * time.Second}

	// List containers belonging to this compose project
	listURL := fmt.Sprintf(
		"http://localhost/containers/json?all=1&filters=%s",
		`{"label":["com.docker.compose.project=`+coinID+`"]}`,
	)
	resp, err := client.Get(listURL)
	if err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "error": "docker socket: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var containers []map[string]interface{}
	if err := json.Unmarshal(body, &containers); err != nil || len(containers) == 0 {
		writeJSON(w, map[string]interface{}{"success": false, "error": "no containers found for " + coinID})
		return
	}

	// Collect container IDs (skip app_proxy — it manages itself)
	var ids []string
	for _, c := range containers {
		names, _ := c["Names"].([]interface{})
		skip := false
		for _, n := range names {
			if name, ok := n.(string); ok && strings.Contains(name, "app_proxy") {
				skip = true
				break
			}
		}
		if !skip {
			if id, ok := c["Id"].(string); ok {
				ids = append(ids, id)
			}
		}
	}

	if len(ids) == 0 {
		writeJSON(w, map[string]interface{}{"success": false, "error": "no non-proxy containers found for " + coinID})
		return
	}

	var lastErr string
	for _, id := range ids {
		var url string
		switch action {
		case "start":
			url = "http://localhost/containers/" + id + "/start"
		case "stop":
			url = "http://localhost/containers/" + id + "/stop?t=10"
		case "restart":
			url = "http://localhost/containers/" + id + "/restart?t=10"
		}
		req, _ := http.NewRequest("POST", url, bytes.NewReader(nil))
		res, err := client.Do(req)
		if err != nil {
			lastErr = err.Error()
			continue
		}
		res.Body.Close()
		if res.StatusCode != 204 && res.StatusCode != 304 {
			lastErr = fmt.Sprintf("container %s: status %d", id[:12], res.StatusCode)
		}
	}

	if lastErr != "" {
		writeJSON(w, map[string]interface{}{"success": false, "error": lastErr})
		return
	}

	statusMap := map[string]string{
		"start":   "running",
		"stop":    "stopped",
		"restart": "running",
	}
	writeJSON(w, map[string]interface{}{"success": true, "status": statusMap[action]})
}
