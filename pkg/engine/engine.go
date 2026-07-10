/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/config"
	"github.com/ForgeNX/forgenx-engine/pkg/noderpc"
	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/metrics"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

const CoinsDir = "/pool/coins"

// Engine is the top-level orchestrator that manages all coin runners.
type Engine struct {
	runners   map[string]*CoinRunner
	runnersMu sync.RWMutex
	stats     *metrics.Stats
	logger    *logging.Logger
	startTime time.Time
	poolName  string
}

// New creates a new Engine from the given configuration.
func New(cfg *config.Config, stats *metrics.Stats) (*Engine, error) {
	e := &Engine{
		runners:   make(map[string]*CoinRunner),
		stats:     stats,
		logger:    logging.New(logging.ModuleEngine),
		startTime: time.Now(),
		poolName:  cfg.PoolName,
	}

	for symbol, coinCfg := range cfg.Coins {
		if !coinCfg.Enabled {
			e.logger.Info("[%s] skipped (disabled)", symbol)
			continue
		}

		runner, err := NewCoinRunner(symbol, coinCfg, cfg.Donation, stats)
		if err != nil {
			return nil, fmt.Errorf("initializing %s: %w", symbol, err)
		}
		e.runners[symbol] = runner
	}

	if len(e.runners) == 0 {
		e.logger.Info("no coins configured at startup — waiting for configs")
	}

	return e, nil
}

// snapshotRunners returns a shallow copy of the runners map, safe to range
// over without holding the lock (avoids blocking readers/writers during
// potentially slow runner.Start()/Stop() calls).
func (e *Engine) snapshotRunners() map[string]*CoinRunner {
	e.runnersMu.RLock()
	defer e.runnersMu.RUnlock()
	snap := make(map[string]*CoinRunner, len(e.runners))
	for symbol, runner := range e.runners {
		snap[symbol] = runner
	}
	return snap
}

// Start begins all coin runners.
func (e *Engine) Start() error {
	var started []string

	snapshot := e.snapshotRunners()

	for symbol, runner := range snapshot {
		if err := runner.Start(); err != nil {
			// Stop any runners that already started
			for _, s := range started {
				if r, ok := snapshot[s]; ok {
					r.Stop()
				}
			}
			return fmt.Errorf("starting %s: %w", symbol, err)
		}
		started = append(started, symbol)
	}

	e.runnersMu.RLock()
	count := len(e.runners)
	e.runnersMu.RUnlock()
	e.logger.Info("engine started with %d coin(s)", count)

	// NEW: scan existing coin configs
	e.LoadExistingCoinConfigs(CoinsDir, config.DonationConfig{})

	return nil
}

// Stop shuts down all coin runners.
func (e *Engine) Stop() {
	snapshot := e.snapshotRunners()
	for symbol, runner := range snapshot {
		e.logger.Info("stopping %s...", symbol)
		runner.Stop()
	}
	e.logger.Info("engine stopped")
}

// Stats returns the metrics stats instance.
func (e *Engine) Stats() *metrics.Stats {
	return e.stats
}

// RunnerCount returns the number of active coin runners.
func (e *Engine) RunnerCount() int {
	e.runnersMu.RLock()
	defer e.runnersMu.RUnlock()
	return len(e.runners)
}

// Sessions returns all active sessions grouped by coin symbol.
func (e *Engine) Sessions() map[string][]stratum.SessionInfo {
	snapshot := e.snapshotRunners()
	result := make(map[string][]stratum.SessionInfo)
	for symbol, runner := range snapshot {
		result[symbol] = runner.Sessions()
	}
	return result
}

// StartCoin dynamically starts a new coin runner.
func (e *Engine) StartCoin(symbol string, coinCfg *config.CoinConfig, donation config.DonationConfig) error {
	e.runnersMu.RLock()
	_, exists := e.runners[symbol]
	e.runnersMu.RUnlock()
	if exists {
		return fmt.Errorf("%s already running", symbol)
	}

	runner, err := NewCoinRunner(symbol, *coinCfg, donation, e.stats)
	if err != nil {
		return err
	}

	if err := runner.Start(); err != nil {
		return err
	}

	e.runnersMu.Lock()
	e.runners[symbol] = runner
	e.runnersMu.Unlock()
	e.logger.Info("[%s] dynamically started", symbol)

	return nil
}

// StopCoin dynamically stops a running coin runner.
func (e *Engine) StopCoin(symbol string) {
	e.runnersMu.RLock()
	runner, exists := e.runners[symbol]
	e.runnersMu.RUnlock()
	if !exists {
		return
	}

	runner.Stop()

	e.logger.Info("[%s] pool stopped", symbol)

	e.runnersMu.Lock()
	delete(e.runners, symbol)
	e.runnersMu.Unlock()
}

// ReloadCoin stops and restarts a coin runner.
func (e *Engine) ReloadCoin(symbol string, coinCfg *config.CoinConfig, donation config.DonationConfig) error {
	e.StopCoin(symbol)
	return e.StartCoin(symbol, coinCfg, donation)
}

// GetNodeStatus returns live node RPC data for a coin symbol by reusing the
// running coin's existing RPC client. The second return value indicates
// whether a runner (and therefore a live RPC client) exists for this symbol.
func (e *Engine) GetNodeStatus(symbol string) (map[string]interface{}, bool) {
	e.runnersMu.RLock()
	runner, exists := e.runners[symbol]
	e.runnersMu.RUnlock()

	if !exists || runner.rpcClient == nil {
		// No runner (e.g. node is in IBD) — try reading coin config directly
		// so the UI can still show sync progress during initial block download.
		cfg, err := loadCoinConfig(CoinsDir + "/" + strings.ToLower(symbol) + ".json")
		if err != nil || cfg == nil {
			return map[string]interface{}{}, false
		}
		tmpRPC := noderpc.NewClient(cfg.Node.Host, cfg.Node.Port, cfg.Node.Username, cfg.Node.Password)
		chain, err := tmpRPC.GetBlockchainInfo()
		if err != nil {
			return map[string]interface{}{"status": "offline", "rpcOnline": false}, false
		}
		progress := chain.VerificationProgress
		if progress > 0.999 {
			progress = 1
		}
		e.stats.SetSyncProgress(symbol, progress)
		return map[string]interface{}{
			"status":                 "online",
			"rpcOnline":              true,
			"chain":                  chain.Chain,
			"blocks":                 chain.Blocks,
			"headers":                chain.Headers,
			"sync_pct":               round2(progress * 100),
			"synced":                 chain.Blocks == chain.Headers,
			"initial_block_download": chain.InitialBlockDownload,
			"best_block_hash":        chain.BestBlockHash,
			"pruned":                 chain.Pruned,
			"connected":              false,
		}, false
	}

	rpc := runner.rpcClient

	chain, err := rpc.GetBlockchainInfo()
	if err != nil {
		return map[string]interface{}{"error": err.Error(), "status": "offline", "rpcOnline": false}, false
	}
	net, _ := rpc.GetNetworkInfo()
	mem, _ := rpc.GetMempoolInfo()
	mining, _ := rpc.GetMiningInfo()

	var lastBlockTime int64
	if chain.BestBlockHash != "" {
		if header, err := rpc.GetBlockHeader(chain.BestBlockHash); err == nil {
			lastBlockTime = header.Time
		}
	}

	nhps := 0.0
	if mining != nil {
		nhps = mining.NetworkHashPS
	}
	var netHashrate string
	switch {
	case nhps >= 1e18:
		netHashrate = fmt.Sprintf("%.2f EH/s", nhps/1e18)
	case nhps >= 1e15:
		netHashrate = fmt.Sprintf("%.2f PH/s", nhps/1e15)
	case nhps >= 1e12:
		netHashrate = fmt.Sprintf("%.2f TH/s", nhps/1e12)
	default:
		netHashrate = fmt.Sprintf("%.0f H/s", nhps)
	}

	info := map[string]interface{}{
		"status":                 "online",
		"rpcOnline":             true,
		"chain":                 chain.Chain,
		"blocks":                 chain.Blocks,
		"headers":                chain.Headers,
		"sync_pct":               round2(chain.VerificationProgress * 100),
		"synced":                 chain.Blocks == chain.Headers,
		"pruned":                 chain.Pruned,
		"prune_height":           chain.PruneHeight,
		"size_on_disk":           chain.SizeOnDisk,
		"best_block_hash":        chain.BestBlockHash,
		"prune_limit_mb":         chain.PruneTargetSize / (1024 * 1024),
		"initial_block_download": chain.InitialBlockDownload,
		"difficulty":             0.0,
		"network_hashrate":       netHashrate,
		"network_hashrate_raw":   nhps,
		"last_block_time":        lastBlockTime,
	}
	if mining != nil {
		info["difficulty"] = mining.Difficulty
	}
	if net != nil {
		info["peers"] = net.Connections
		info["peers_in"] = net.ConnectionsIn
		info["peers_out"] = net.ConnectionsOut
		info["version"] = net.Subversion
	}
	if mem != nil {
		info["mempool_txns"] = mem.Size
		info["mempool_size_mb"] = round2(float64(mem.Usage) / 1024 / 1024)
	}

	return info, true
}

func round2(f float64) float64 {
	return math.Round(f*100) / 100
}

// GetDonationAddress returns the donation address for a coin symbol and network
// from the embedded AUTHORS file.
func (e *Engine) GetDonationAddress(symbol, network string) (string, error) {
	return loadDonationAddress(symbol, network)
}

// LoadExistingCoinConfigs scans the coin config directory and starts pools for existing configs.
func (e *Engine) LoadExistingCoinConfigs(dir string, donation config.DonationConfig) {

	files, err := os.ReadDir(dir)
	if err != nil {
		e.logger.Warn("could not scan coin config directory: %v", err)
		return
	}

	for _, file := range files {

		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		path := filepath.Join(dir, file.Name())

		data, err := os.ReadFile(path)
		if err != nil {
			e.logger.Warn("failed reading %s: %v", file.Name(), err)
			continue
		}

		var coinCfg config.CoinConfig
		if err := json.Unmarshal(data, &coinCfg); err != nil {
			e.logger.Warn("invalid config %s: %v", file.Name(), err)
			continue
		}

		// use symbol from filename (your current logic)
		symbol := strings.ToUpper(strings.TrimSuffix(file.Name(), ".json"))

		// 🔥 owner_app check with retry
		if coinCfg.OwnerApp != "" {
			appPath := "/opt/forgenx/apps/" + coinCfg.OwnerApp

			exists := false

			// try for ~20 seconds (40 × 500ms)
			for i := 0; i < 40; i++ {
				if _, err := os.Stat(appPath); err == nil {
					exists = true
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			if !exists {
				e.logger.Warn("[%s] owner app CONFIRMED missing after retries — deleting config (%s)", symbol, appPath)

				if err := os.Remove(path); err != nil {
					e.logger.Error("[%s] failed to delete config %s: %v", symbol, path, err)
				}

				continue
			}

			e.logger.Info("[%s] owner app verified: %s", symbol, appPath)
		}

		e.handleCoinConfig(symbol, &coinCfg, donation)
	}
}

func (e *Engine) HandleFleet(w http.ResponseWriter, r *http.Request) {

	totalWorkers := 0

	sessions := e.Sessions()
	for _, list := range sessions {
		totalWorkers += len(list)
	}

	totalHashrate := 0.0

	coins := []map[string]interface{}{}

	coinStats := e.stats.GetCoinsSnapshot()

	// 🔥 ensure we include coins even if stats missing
	seen := make(map[string]bool)

	for symbol, stats := range coinStats {

		workers := 0
		if list, ok := sessions[symbol]; ok {
			workers = len(list)
		}

		hashrate := 0.0
		e.runnersMu.RLock()
		runner, ok := e.runners[symbol]
		e.runnersMu.RUnlock()
		if ok {
			hashrate = runner.Hashrate()
			e.stats.UpdateMaxPoolHashrate(symbol, hashrate)
			totalHashrate += hashrate
		}

		coins = append(coins, map[string]interface{}{
			"symbol":            symbol,
			"workers":           workers,
			"hashrate":          hashrate,
			"sync_progress":     stats.SyncProgress,
			"max_pool_hashrate": stats.MaxPoolHashrate,
		})

		seen[symbol] = true
	}

	// 🔥 fallback: include coins from sessions (if stats not ready)
	for symbol, list := range sessions {

		if seen[symbol] {
			continue
		}

		coins = append(coins, map[string]interface{}{
			"symbol":        symbol,
			"workers":       len(list),
			"hashrate":      0,
			"sync_progress": 0,
		})
	}

	resp := map[string]interface{}{
		"total_hashrate": totalHashrate,
		"total_workers":  totalWorkers,
		"coins":          coins,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetCoinPortStatus returns whether the V1 and V2 stratum ports are live for a coin.
func (e *Engine) GetCoinPortStatus(symbol string) (v1Running, v2Running bool) {
	e.runnersMu.RLock()
	runner, exists := e.runners[symbol]
	e.runnersMu.RUnlock()
	if !exists {
		return false, false
	}
	return runner.StratumRunning(), runner.SV2Running()
}
