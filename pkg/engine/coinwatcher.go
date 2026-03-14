package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/mmfpsolutions/gostratumengine/pkg/config"
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

			        // Only react to CREATE, WRITE, REMOVE
				if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				        continue
				}

				if !strings.HasSuffix(event.Name, ".json") {
					continue
				}

				symbol := strings.ToUpper(
					strings.TrimSuffix(filepath.Base(event.Name), ".json"),
				)

				now := time.Now()

				if t, ok := lastEvent[symbol]; ok {
				    if now.Sub(t) < time.Second {
				        continue
				    }
				}

				lastEvent[symbol] = now

				if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {

				        // Give the editor time to finish writing the file
				        time.Sleep(200 * time.Millisecond)

				        cfg, err := loadCoinConfig(event.Name)
				        if err != nil {
				                e.logger.Error("[%s] config load failed: %v", symbol, err)
				                continue
				        }

				        if _, exists := e.runners[symbol]; exists {

				                e.logger.Info("[%s] config changed — reloading pool", symbol)
				                e.ReloadCoin(symbol, cfg, donation)

				        } else {

				                e.logger.Info("[%s] config detected — starting pool", symbol)
				                e.StartCoin(symbol, cfg, donation)

				        }
				}

				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {

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
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
