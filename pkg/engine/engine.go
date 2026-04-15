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
	"fmt"
        "encoding/json"
        "net/http"
        "os"
        "path/filepath"
	"strings"
	"time"

	"github.com/mmfpsolutions/gostratumengine/pkg/config"
	"github.com/mmfpsolutions/gostratumengine/pkg/logging"
	"github.com/mmfpsolutions/gostratumengine/pkg/metrics"
	"github.com/mmfpsolutions/gostratumengine/pkg/stratum"
)

const CoinsDir = "/pool/coins"

// Engine is the top-level orchestrator that manages all coin runners.
type Engine struct {
	runners map[string]*CoinRunner
	stats   *metrics.Stats
	logger  *logging.Logger
	startTime time.Time
        poolName string
}

// New creates a new Engine from the given configuration.
func New(cfg *config.Config, stats *metrics.Stats) (*Engine, error) {
	e := &Engine{
		runners: make(map[string]*CoinRunner),
		stats:   stats,
		logger:  logging.New(logging.ModuleEngine),
	        startTime: time.Now(),
	        poolName: cfg.PoolName,
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

// Start begins all coin runners.
func (e *Engine) Start() error {
    var started []string

    for symbol, runner := range e.runners {
        if err := runner.Start(); err != nil {
            // Stop any runners that already started
            for _, s := range started {
                e.runners[s].Stop()
            }
            return fmt.Errorf("starting %s: %w", symbol, err)
        }
        started = append(started, symbol)
    }

    e.logger.Info("engine started with %d coin(s)", len(e.runners))

    // NEW: scan existing coin configs
    e.LoadExistingCoinConfigs(CoinsDir, config.DonationConfig{})

    return nil
}

// Stop shuts down all coin runners.
func (e *Engine) Stop() {
	for symbol, runner := range e.runners {
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
	return len(e.runners)
}

// Sessions returns all active sessions grouped by coin symbol.
func (e *Engine) Sessions() map[string][]stratum.SessionInfo {
    result := make(map[string][]stratum.SessionInfo)
    for symbol, runner := range e.runners {
        result[symbol] = runner.Sessions()
    }
    return result
}


// StartCoin dynamically starts a new coin runner.
func (e *Engine) StartCoin(symbol string, coinCfg *config.CoinConfig, donation config.DonationConfig) error {
    if _, exists := e.runners[symbol]; exists {
        return fmt.Errorf("%s already running", symbol)
    }

    runner, err := NewCoinRunner(symbol, *coinCfg, donation, e.stats)
    if err != nil {
        return err
    }

    if err := runner.Start(); err != nil {
        return err
    }

    e.runners[symbol] = runner
    e.logger.Info("[%s] dynamically started", symbol)

    return nil
}

// StopCoin dynamically stops a running coin runner.
func (e *Engine) StopCoin(symbol string) {
    runner, exists := e.runners[symbol]
    if !exists {
        return
    }

    runner.Stop()

    e.logger.Info("[%s] pool stopped", symbol)

    delete(e.runners, symbol)
}

// ReloadCoin stops and restarts a coin runner.
func (e *Engine) ReloadCoin(symbol string, coinCfg *config.CoinConfig, donation config.DonationConfig) error {
    e.StopCoin(symbol)
    return e.StartCoin(symbol, coinCfg, donation)
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
            appPath := "/var/lib/5tratumos/apps/" + coinCfg.OwnerApp

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

    totalHashrate := 0.0 // still placeholder

    coins := []map[string]interface{}{}

    coinStats := e.stats.GetCoinsSnapshot()

    // 🔥 ensure we include coins even if stats missing
    seen := make(map[string]bool)

    for symbol, stats := range coinStats {

        workers := 0
        if list, ok := sessions[symbol]; ok {
            workers = len(list)
        }

        coins = append(coins, map[string]interface{}{
            "symbol":        symbol,
            "workers":       workers,
            "hashrate":      0,
            "sync_progress": stats.SyncProgress,
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
