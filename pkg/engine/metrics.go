package engine

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
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

	// MinersConnected counts distinct workers, not sessions. A miner bonded to
	// several coins through Nexus Mesh holds an authorized session on each — one
	// active, the rest warm — so summing the per-coin counts reported the same
	// physical miner once per coin. Per-coin figures stay as they are: a warm
	// session really is connected to that coin.
	distinct := make(map[string]struct{})

	for symbol, runner := range e.runners {

		sessions := runner.Sessions()
		miners := len(sessions)

		// temporary hashrate calculation
		hashrate := math.Round(runner.Hashrate()*100) / 100

		for _, sess := range sessions {
			// Key on the worker suffix: the same miner authorizes as
			// <dgb-address>.Ellevix002 on one coin and <bch-address>.Ellevix002 on
			// another, so the full authorize strings never match.
			name := sess.WorkerName
			if name == "" {
				continue
			}
			if i := strings.LastIndex(name, "."); i >= 0 && i+1 < len(name) {
				name = name[i+1:]
			}
			distinct[name] = struct{}{}
		}

		metrics.Coins[symbol] = CoinMetrics{
			Miners:   miners,
			Hashrate: hashrate,
		}
	}

	metrics.MinersConnected = len(distinct)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
