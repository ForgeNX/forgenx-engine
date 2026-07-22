package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ForgeNX/forgenx-engine/pkg/config"
	"github.com/ForgeNX/forgenx-engine/pkg/noderpc"
)

func (e *Engine) WatchCoins(dir string, donation config.DonationConfig) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	err = watcher.Add(dir)
	if err != nil {
		return err
	}

	e.logger.Info("watching %s for coin configs", dir)

	go func() {
		lastEvent := make(map[string]time.Time)

		for {
			select {

			case event := <-watcher.Events:

				// Only react to CREATE, WRITE, REMOVE, RENAME
				if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
					continue
				}

				if !strings.HasSuffix(event.Name, ".json") {
					continue
				}

				fileKey := event.Name
				now := time.Now()

				// debounce per file (not symbol anymore)
				if t, ok := lastEvent[fileKey]; ok {
					if now.Sub(t) < time.Second {
						continue
					}
				}
				lastEvent[fileKey] = now

				// ===== CREATE / WRITE =====
				if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {

					// Give the editor time to finish writing the file
					time.Sleep(200 * time.Millisecond)

					cfg, err := loadCoinConfig(event.Name)
					if err != nil {
						e.logger.Error("config load failed: %v", err)
						continue
					}

					// symbol now comes from config
					symbol := strings.ToUpper(
						strings.TrimSuffix(filepath.Base(event.Name), ".json"),
					)

					e.handleCoinConfig(symbol, cfg, donation)
				}

				// ===== REMOVE / RENAME =====
				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {

					// fallback: derive symbol from filename
					symbol := strings.ToUpper(
						strings.TrimSuffix(filepath.Base(event.Name), ".json"),
					)

					e.logger.Info("[%s] config removed — stopping pool", symbol)

					e.StopCoin(symbol)
				}

			case err := <-watcher.Errors:
				e.logger.Error("watcher error: %v", err)
			}
		}
	}()

	return nil
}

func loadCoinConfig(path string) (*config.CoinConfig, error) {

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg config.CoinConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

	func (e *Engine) handleCoinConfig(symbol string, cfg *config.CoinConfig, donation config.DonationConfig) {

	// ALWAYS register coin first (even if syncing/offline)
	e.stats.InitCoin(symbol)

	if !cfg.Enabled {
		e.logger.Info("[%s] disabled — stopping pool", symbol)
		e.StopCoin(symbol)
		return
	}

	// Live RPC check — engine determines node state itself, no external flags needed.
	// This makes the engine fully self-contained on Umbrel OS and other platforms
	// where there is no forgenxd process to update ibd_complete/node_online in the config.
	rpc := noderpc.NewClient(cfg.Node.Host, cfg.Node.Port, cfg.Node.Username, cfg.Node.Password)
	chain, err := rpc.GetBlockchainInfo()
	if err != nil {
		e.logger.Info("[%s] node offline (RPC unreachable: %v) — pool not started/stopped", symbol, err)
		e.runnersMu.RLock()
		_, exists := e.runners[symbol]
		e.runnersMu.RUnlock()
		if exists {
			e.StopCoin(symbol)
		}
		return
	}

	// Update sync progress
	progress := chain.VerificationProgress
	if progress > 0.999 {
		progress = 1
	}
	e.stats.SetSyncProgress(symbol, progress)

	if chain.InitialBlockDownload {
		e.runnersMu.RLock()
		_, exists := e.runners[symbol]
		e.runnersMu.RUnlock()
		if exists {
			e.logger.Info("[%s] node syncing (IBD) — stopping pool", symbol)
			e.StopCoin(symbol)
		} else {
			e.logger.Info("[%s] node syncing (IBD) — pool not started", symbol)
		}
		return
	}


	// Wait for at least 1 peer before starting pool — prevents "not connected" errors
	netInfo, err := rpc.GetNetworkInfo()
	if err == nil && netInfo.Connections == 0 {
		e.logger.Info("[%s] node has no peers yet — waiting before starting pool", symbol)
		return
	}
	if err == nil && netInfo.Connections > 0 {
		e.logger.Info("[%s] node has %d peer(s) — proceeding to start pool", symbol, netInfo.Connections)
	}
	e.runnersMu.RLock()
	_, exists := e.runners[symbol]
	e.runnersMu.RUnlock()
	if exists {
		e.logger.Info("[%s] config valid — reloading pool", symbol)
		e.ReloadCoin(symbol, cfg, donation)
	} else {
		e.logger.Info("[%s] config valid — starting pool", symbol)
		if err := e.StartCoin(symbol, cfg, donation); err != nil {
			e.logger.Error("[%s] failed to start pool: %v", symbol, err)
		}
	}
}
