package coinapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// CoinAPI handles HTTP requests for coin app endpoints.
type CoinAPI struct {
	store          *Store
	engineAPIURL   string // e.g. "http://localhost:8080"
	nodeRPCFunc    NodeRPCFunc
	coinConfigFunc CoinConfigFunc
	logsFunc       LogsFunc
}

// NodeRPCFunc fetches node info for a coin symbol.
type NodeRPCFunc func(symbol string) map[string]interface{}

// CoinConfigFunc fetches coin settings for a coin symbol.
type CoinConfigFunc func(symbol string) map[string]interface{}

// LogsFunc fetches recent logs for a coin.
type LogsFunc func(symbol string, tail int) []string

func NewCoinAPI(store *Store, engineAPIURL string) *CoinAPI {
	return &CoinAPI{
		store:        store,
		engineAPIURL: engineAPIURL,
	}
}

func (c *CoinAPI) SetNodeRPCFunc(f NodeRPCFunc)       { c.nodeRPCFunc = f }
func (c *CoinAPI) SetCoinConfigFunc(f CoinConfigFunc) { c.coinConfigFunc = f }
func (c *CoinAPI) SetLogsFunc(f LogsFunc)             { c.logsFunc = f }

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
	if c.nodeRPCFunc != nil {
		nodeInfo = c.nodeRPCFunc(symbol)
	}

	writeJSON(w, map[string]interface{}{
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
		default:
			// Forward to node RPC or return 404
			fmt.Fprintf(w, `{"error":"not implemented"}`)
		}
	})
}
