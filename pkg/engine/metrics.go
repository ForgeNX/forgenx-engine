package engine

import (
	"encoding/json"
	"math"
	"net/http"
	"time"
)

type CoinMetrics struct {
	Miners   int     `json:"miners"`
	Hashrate float64 `json:"hashrate"`
}

type Metrics struct {
	PoolName        string                 `json:"pool_name"`
	UptimeSeconds   int64                  `json:"uptime_seconds"`
	PoolsRunning    int                    `json:"pools_running"`
	MinersConnected int                    `json:"miners_connected"`
	Coins           map[string]CoinMetrics `json:"coins"`
}

func (e *Engine) MetricsHandler(w http.ResponseWriter, r *http.Request) {

	metrics := Metrics{
		PoolName:      e.poolName,
		UptimeSeconds: int64(time.Since(e.startTime) / time.Second),
		PoolsRunning:  len(e.runners),
		Coins:         make(map[string]CoinMetrics),
	}

	for symbol, runner := range e.runners {

		miners := len(runner.Sessions())

		// temporary hashrate calculation
		hashrate := math.Round(runner.Hashrate()*100) / 100

		metrics.MinersConnected += miners

		metrics.Coins[symbol] = CoinMetrics{
			Miners:   miners,
			Hashrate: hashrate,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
