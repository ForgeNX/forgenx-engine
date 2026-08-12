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
		m.DefaultAllocation = out
	}
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

	// Nexus Mesh — opt-in. Enabled via environment (MESH_ENABLED=true), because
	// this deployment has no top-level config.json (config.Load returns defaults;
	// coins load from /pool/coins). Env is how the compose injects mesh settings.
	// The engine behaves exactly as a non-mesh engine unless MESH_ENABLED=true.
	applyMeshEnv(&cfg.Mesh)
	if cfg.Mesh.Enabled {
		rotate, _ := time.ParseDuration(cfg.Mesh.RotateInterval)
		nexusMesh := mesh.New(eng, mesh.Options{
			Host:           "0.0.0.0",
			Port:           cfg.Mesh.Port,
			DefaultDiff:    cfg.Mesh.DefaultDiff,
			RotateInterval: rotate,
		})
		// Apply the configured default allocation to every miner that connects,
		// until the UI sets a custom split. Wired via the mesh's onAuthorized path
		// by pre-seeding here through SetAllocation in the scheduler once a session
		// appears — for the first bring-up we set it in a lightweight watcher.
		defAlloc := make([]mesh.CoinWeight, 0, len(cfg.Mesh.DefaultAllocation))
		for _, w := range cfg.Mesh.DefaultAllocation {
			defAlloc = append(defAlloc, mesh.CoinWeight{Coin: w.Coin, Percent: w.Percent})
		}
		nexusMesh.SetDefaultAllocation(defAlloc)
		go func() {
			if err := nexusMesh.Start(); err != nil {
				logger.Error("Nexus Mesh start failed: %v", err)
			}
		}()
		logger.Info("Nexus Mesh enabled on port %d (rotate=%s, default coins=%d)",
			cfg.Mesh.Port, cfg.Mesh.RotateInterval, len(defAlloc))
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
