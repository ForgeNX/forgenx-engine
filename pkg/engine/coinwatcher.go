package engine

import (
	"crypto/sha256"
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

// configSignature computes a SHA256 hash of all meaningful pool config fields,
// excluding runtime-only fields written by the entrypoint (ibd_complete, node_online, enabled).
func configSignature(cfg *config.CoinConfig) [32]byte {
	type sig struct {
		Mining   interface{}
		Donation interface{}
		VarDiff  interface{}
		Stratum  interface{}
		Node     interface{}
	}
	s := sig{
		Mining:   cfg.Mining,
		Donation: cfg.Donation,
		VarDiff:  cfg.VarDiff,
		Stratum:  cfg.Stratum,
		Node:     cfg.Node,
	}
	data, _ := json.Marshal(s)
	return sha256.Sum256(data)
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
	newSig := configSignature(cfg)
	e.configSigsMu.RLock()
	oldSig, hasSig := e.configSigs[symbol]
	e.configSigsMu.RUnlock()
	e.runnersMu.RLock()
	_, exists := e.runners[symbol]
	e.runnersMu.RUnlock()
	if exists {
		if hasSig && oldSig == newSig {
			// Config unchanged — no reload needed
			return
		}
		// Log what changed
		e.cfgsMu.RLock()
		oldCfg, hasOldCfg := e.prevCfgs[symbol]
		e.cfgsMu.RUnlock()
		if hasOldCfg {
			if oldCfg.VarDiff.TargetTime != cfg.VarDiff.TargetTime {
				e.logger.Info("[%s] config: vardiff target_time %.0fs → %.0fs", symbol, oldCfg.VarDiff.TargetTime, cfg.VarDiff.TargetTime)
			}
			if oldCfg.VarDiff.RetargetTime != cfg.VarDiff.RetargetTime {
				e.logger.Info("[%s] config: vardiff retarget_time %.0fs → %.0fs", symbol, oldCfg.VarDiff.RetargetTime, cfg.VarDiff.RetargetTime)
			}
			if oldCfg.VarDiff.MinDiff != cfg.VarDiff.MinDiff {
				e.logger.Info("[%s] config: vardiff min_diff %.0f → %.0f", symbol, oldCfg.VarDiff.MinDiff, cfg.VarDiff.MinDiff)
			}
			if oldCfg.VarDiff.MaxDiff != cfg.VarDiff.MaxDiff {
				e.logger.Info("[%s] config: vardiff max_diff %.0f → %.0f", symbol, oldCfg.VarDiff.MaxDiff, cfg.VarDiff.MaxDiff)
			}
			if oldCfg.VarDiff.VariancePct != cfg.VarDiff.VariancePct {
				e.logger.Info("[%s] config: vardiff variance_pct %.0f%% → %.0f%%", symbol, oldCfg.VarDiff.VariancePct, cfg.VarDiff.VariancePct)
			}
			if oldCfg.Mining.Address != cfg.Mining.Address {
				e.logger.Info("[%s] config: payout address changed", symbol)
			}
			if oldCfg.Mining.Network != cfg.Mining.Network {
				e.logger.Info("[%s] config: network %s → %s", symbol, oldCfg.Mining.Network, cfg.Mining.Network)
			}
			if oldCfg.Stratum.ConnectionTimeout != cfg.Stratum.ConnectionTimeout {
				e.logger.Info("[%s] config: connection_timeout %ds → %ds", symbol, oldCfg.Stratum.ConnectionTimeout, cfg.Stratum.ConnectionTimeout)
			}
			if oldCfg.Stratum.AcceptSuggestDiff != cfg.Stratum.AcceptSuggestDiff {
				e.logger.Info("[%s] config: accept_suggest_diff %v → %v", symbol, oldCfg.Stratum.AcceptSuggestDiff, cfg.Stratum.AcceptSuggestDiff)
			}
			if oldCfg.Stratum.LowDiffShareGrace != cfg.Stratum.LowDiffShareGrace {
				e.logger.Info("[%s] config: low_diff_share_grace %ds → %ds", symbol, oldCfg.Stratum.LowDiffShareGrace, cfg.Stratum.LowDiffShareGrace)
			}
			if oldCfg.VarDiff.OnNewBlock != nil && cfg.VarDiff.OnNewBlock != nil && *oldCfg.VarDiff.OnNewBlock != *cfg.VarDiff.OnNewBlock {
				e.logger.Info("[%s] config: vardiff on_new_block %v → %v", symbol, *oldCfg.VarDiff.OnNewBlock, *cfg.VarDiff.OnNewBlock)
			}
		}
		e.cfgsMu.Lock()
		cfgCopy := *cfg
		e.prevCfgs[symbol] = &cfgCopy
		e.cfgsMu.Unlock()
		e.logger.Info("[%s] config changed — reloading pool", symbol)
		e.configSigsMu.Lock()
		e.configSigs[symbol] = newSig
		e.configSigsMu.Unlock()
		e.ReloadCoin(symbol, cfg, donation)
	} else {
		e.logger.Info("[%s] config valid — starting pool", symbol)
		e.cfgsMu.Lock()
		cfgCopy := *cfg
		e.prevCfgs[symbol] = &cfgCopy
		e.cfgsMu.Unlock()
		e.configSigsMu.Lock()
		e.configSigs[symbol] = newSig
		e.configSigsMu.Unlock()
		if err := e.StartCoin(symbol, cfg, donation); err != nil {
			e.logger.Error("[%s] failed to start pool: %v", symbol, err)
		}
	}
}
