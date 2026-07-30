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
	"syscall"

	// Import coin packages to trigger init() registration
	_ "github.com/ForgeNX/forgenx-engine/pkg/coin"

	"github.com/ForgeNX/forgenx-engine/pkg/coinapi"
	"github.com/ForgeNX/forgenx-engine/pkg/config"
	"github.com/ForgeNX/forgenx-engine/pkg/engine"
	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/metrics"
)

var (
	version   = "dev"
	buildDate = "unknown"
	commit    = "unknown"
)

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
