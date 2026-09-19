/*
 * Copyright 2026 ForgeNX
  *
   * This program is free software; you can redistribute it and/or modify it
    * under the terms of the GNU General Public License as published by the Free
	 * Software Foundation; either version 3 of the License, or (at your option)
	  * any later version. See LICENSE for more details.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	// Import coin packages to trigger init() registration
	_ "github.com/ForgeNX/forgenx-engine/pkg/coin"

	"github.com/ForgeNX/forgenx-engine/pkg/coinapi"
	"github.com/ForgeNX/forgenx-engine/pkg/config"
	"github.com/ForgeNX/forgenx-engine/pkg/engine"
	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/mesh"
	"github.com/ForgeNX/forgenx-engine/pkg/metrics"
)

var (
	version   = "dev"
	buildDate = "unknown"
	commit    = "unknown"
)

// applyMeshEnv overlays Nexus Mesh settings from environment variables onto the
// (otherwise default/empty) mesh config. Used because this deployment configures
// the engine via env + /pool/coins rather than a top-level config.json.
//
//	MESH_ENABLED=true|false
//	MESH_PORT=3350
//	MESH_ROTATE_INTERVAL=20s
//	MESH_DEFAULT_DIFF=1024
//	MESH_DEFAULT_ALLOCATION=DGB:100   (comma-separated COIN:PCT pairs)
func applyMeshEnv(m *config.MeshConfig) {
	if v := os.Getenv("MESH_ENABLED"); v == "true" || v == "1" {
		m.Enabled = true
	}
	if v := os.Getenv("MESH_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			m.Port = p
		}
	}
	if v := os.Getenv("MESH_ROTATE_INTERVAL"); v != "" {
		m.RotateInterval = v
	}
	if v := os.Getenv("MESH_DEFAULT_DIFF"); v != "" {
		if d, err := strconv.ParseFloat(v, 64); err == nil {
			m.DefaultDiff = d
		}
	}
	if v := os.Getenv("MESH_DEFAULT_ALLOCATION"); v != "" {
		m.DefaultAllocation = parseAllocation(v)
	}
}

// Rotation cycle bounds. Below fifteen minutes a miner spends too much of each
// slice on partial work for the split to mean much; above six hours a "rotating"
// miner looks stuck on one coin for most of a day.
const (
	minRotateCycle     = 15 * time.Minute
	maxRotateCycle     = 6 * time.Hour
	defaultRotateCycle = 1 * time.Hour
)

func clampRotateCycle(d time.Duration) time.Duration {
	if d < minRotateCycle {
		return minRotateCycle
	}
	if d > maxRotateCycle {
		return maxRotateCycle
	}
	return d
}

// parseAllocation reads "DGB:50,BCH:50" into weights. Used for both the mesh-wide
// default and each miner's stored assignment, so the two can never drift in how
// they are interpreted. Malformed pairs are skipped rather than failing the whole
// string — a bad character in one entry should not silently unassign a miner.
func parseAllocation(v string) []config.MeshWeight {
	var out []config.MeshWeight
	for _, pair := range strings.Split(v, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) != 2 {
			continue
		}
		pct, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		out = append(out, config.MeshWeight{Coin: strings.ToUpper(parts[0]), Percent: pct})
	}
	return out
}

func main() {
	configPath := flag.String("config", "config.json", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("ForgeNX Engine %s (commit: %s, built: %s)\n", version, commit, buildDate)
		os.Exit(0)
	}

	logger := logging.New(logging.ModuleMain)

	// Banner
	fmt.Println("\u256c\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2563")
	fmt.Println("\u2551           ForgeNX Engine              \u2551")
	fmt.Printf("\u2551 Version: %-24s\u2551\n", version)
	fmt.Println("\u2551   Multi-coin Stratum V1 Engine       \u2551")
	fmt.Println("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d")
	fmt.Println()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("configuration error: %v", err)
	}

	// Set log level
	logging.SetGlobalLevel(cfg.LogLevel)
	// Durable file logging: write logs to a file on the persisted /pool/logs
	// volume (in addition to stdout) so they survive container recreation on
	// redeploys. Path is overridable via FORGENX_LOG_FILE; empty disables it.
	logFilePath := os.Getenv("FORGENX_LOG_FILE")
	if logFilePath == "" {
		logFilePath = "/pool/logs/engine.log"
	}
	logging.SetLogFile(logFilePath)
	logger.Info("pool: %s | log level: %s | log file: %s", cfg.PoolName, cfg.LogLevel, logFilePath)

	// Create stats
	stats := metrics.NewStats()

	// Create and start the engine
	eng, err := engine.New(cfg, stats)
	if err != nil {
		logger.Fatal("engine initialization: %v", err)
	}

	if err := os.MkdirAll(engine.CoinsDir, 0755); err != nil {
		logger.Fatal("failed to create coins directory: %v", err)
	}

	if err := eng.Start(); err != nil {
		logger.Fatal("engine start: %v", err)
	}

	eng.WatchCoins(engine.CoinsDir, cfg.Donation)
	eng.StartNodeRetryLoop(engine.CoinsDir, cfg.Donation)

	// Declared out here so the per-miner assignment lookup can be attached once the
	// store exists, further down. nil when the mesh is disabled.
	var nexusMesh *mesh.Mesh

	// Nexus Mesh — opt-in. Enabled via environment (MESH_ENABLED=true), because
	// this deployment has no top-level config.json (config.Load returns defaults;
	// coins load from /pool/coins). Env is how the compose injects mesh settings.
	// The engine behaves exactly as a non-mesh engine unless MESH_ENABLED=true.
	applyMeshEnv(&cfg.Mesh)
	if cfg.Mesh.Enabled {
		// Bond order comes from the configured allocation (e.g. "DGB:50,BCH:50" ->
		// ["DGB","BCH"]). The first coin that resolves and is running becomes active;
		// the rest are held warm for rotation. A single entry keeps the original
		// single-coin behaviour.
		coins := make([]string, 0, len(cfg.Mesh.DefaultAllocation))
		for _, a := range cfg.Mesh.DefaultAllocation {
			if a.Coin != "" {
				coins = append(coins, a.Coin)
			}
		}
		if len(coins) == 0 {
			coins = []string{"DGB"}
		}
		// Resolver hands the relay each coin's real V1 stratum endpoint. The
		// relay dials it as an ordinary client — no internal coin APIs touched.
		resolve := func(symbol string) (host string, port int, payout string, running bool, ok bool) {
			eps := eng.NexusEndpoints()
			ep, found := eps[symbol]
			if !found {
				return "", 0, "", false, false
			}
			return "127.0.0.1", ep.V1Port, ep.Payout, ep.V1Running, true
		}
		nexusMesh = mesh.New(mesh.Options{
			Port:    cfg.Mesh.Port,
			Coins:   coins,
			Resolve: resolve,
		})
		if err := nexusMesh.Start(); err != nil {
			logger.Error("Nexus Mesh start failed: %v", err)
		} else {
			logger.Info("Nexus Mesh enabled on port %d (coins=%v)", cfg.Mesh.Port, coins)
		}
	}

	// Start metrics API
	api := metrics.NewAPIServer(cfg.APIPort, cfg.PoolName, stats)
	api.SetSessionProvider(eng.Sessions)
	api.SetMetricsHandler(eng.MetricsHandler)
	api.SetFleetHandler(eng.HandleFleet)
	api.SetPoolRatioHandler(eng.HandlePoolRatio)
	if err := api.Start(); err != nil {
		logger.Fatal("metrics API start: %v", err)
	}
	// Start CoinAPI
	engineAPIURL := fmt.Sprintf("http://localhost:%d", cfg.APIPort)
	store, storeErr := coinapi.NewStore(cfg.DBPath)
	if storeErr != nil {
		logger.Warn("CoinAPI store init failed: %v", storeErr)
	} else {
		coinAPI := coinapi.NewCoinAPI(store, engineAPIURL)
		coinAPI.SetStats(stats)

		// Per-miner allocation, now that the store exists. The mesh is already
		// listening; assignments are read at authorize, so a miner that connected in
		// between mines the default until it next reconnects.
		if nexusMesh != nil {
			nexusMesh.SetAssignmentLookup(func(worker string) ([]mesh.Weight, bool) {
				alloc, found := store.GetMeshAssignment(worker)
				if !found {
					return nil, false
				}
				parsed := parseAllocation(alloc)
				if len(parsed) == 0 {
					return nil, false
				}
				out := make([]mesh.Weight, 0, len(parsed))
				for _, w := range parsed {
					out = append(out, mesh.Weight{Coin: w.Coin, Percent: w.Percent})
				}
				return out, true
			})
			coinAPI.SetMeshReassign(func(worker string, pairs [][2]interface{}) (int, error) {
				weights := make([]mesh.Weight, 0, len(pairs))
				for _, p := range pairs {
					coin, _ := p[0].(string)
					pct, _ := p[1].(float64)
					weights = append(weights, mesh.Weight{Coin: coin, Percent: pct})
				}
				return nexusMesh.ReassignWorker(worker, weights)
			})
			coinAPI.SetMeshActiveCoins(nexusMesh.ActiveCoins)
			coinAPI.SetMeshMinerFacts(nexusMesh.MinerFacts)
			coinAPI.SetMeshPending(nexusMesh.PendingSwitches)
			nexusMesh.SetDefaultLookup(func() ([]string, bool) {
				alloc, ok := store.GetMeshDefault()
				if !ok {
					return nil, false
				}
				parsed := parseAllocation(alloc)
				if len(parsed) == 0 {
					return nil, false
				}
				order := make([]string, 0, len(parsed))
				for _, w := range parsed {
					order = append(order, w.Coin)
				}
				return order, true
			})
			nexusMesh.SetRotateCycleLookup(func() time.Duration {
				d := defaultRotateCycle
				if v, ok := store.GetMeshInterval(); ok {
					if parsed, err := time.ParseDuration(v); err == nil {
						d = parsed
					}
				}
				return clampRotateCycle(d)
			})
			coinAPI.SetMeshInfo(func() (bool, int, []string) {
				return nexusMesh.Enabled(), nexusMesh.Port(), nexusMesh.Coins()
			})
		}
		coinAPI.SetEngineVersion(version, buildDate)
		eng.SetStore(store)

		coinAPI.SetNodeRPCFunc(func(symbol string) map[string]interface{} {

			info, connected := eng.GetNodeStatus(symbol)

			info["connected"] = connected

			return info

		})
		coinAPI.SetBlockConfFunc(eng.GetBlockConfirmations)
		coinAPI.SetDonationFunc(eng.GetDonationAddress)
		coinAPI.SetReloadCoinFunc(func(sym string) error { return eng.ReloadCoinBySymbol(sym, cfg.Donation) })
		coinAPI.SetPortStatusFunc(eng.GetCoinPortStatus)

		coinAPI.RegisterRoutes(api.Mux())
		coinAPI.StartSnapshotThread()
		logger.Info("CoinAPI started, DB: %s", cfg.DBPath)
	}

	logger.Info("ForgeNX Engine is running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("received signal %s, shutting down...", sig)

	// Graceful shutdown
	api.Stop()
	eng.Stop()

	logger.Info("ForgeNX Engine stopped. Goodbye!")
}
