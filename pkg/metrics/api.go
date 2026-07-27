/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
)

// SessionProvider returns active sessions grouped by coin symbol.
type SessionProvider func() map[string][]stratum.SessionInfo

// PoolStatsResponse is the JSON response for GET /stats.
type PoolStatsResponse struct {
	PoolName      string                `json:"pool_name"`
	UptimeSeconds float64               `json:"uptime_seconds"`
	Coins         map[string]*CoinStats `json:"coins"`
}

// MinerInfo combines live session info with historical worker stats.
type MinerInfo struct {
	WorkerName            string    `json:"worker_name"`
	RemoteAddr            string    `json:"remote_addr"`
	Difficulty            float64   `json:"difficulty"`
	ConnectedAt           time.Time `json:"connected_at"`
	SharesAccepted        uint64    `json:"shares_accepted"`
	SharesRejected        uint64    `json:"shares_rejected"`
	SharesStale           uint64    `json:"shares_stale"`
	SessionSharesAccepted uint64    `json:"session_shares_accepted"`
	SessionSharesRejected uint64    `json:"session_shares_rejected"`
	Protocol              string    `json:"protocol"`
	BestDifficultySession float64   `json:"best_difficulty_session"`
	LastSeenTime          string    `json:"last_seen_time,omitempty"`
	BlocksFound           uint64    `json:"blocks_found"`
	LastShareTime         time.Time `json:"last_share_time,omitempty"`
	BestDifficulty        float64   `json:"best_difficulty"`
	BestNetworkDiff       float64   `json:"best_network_diff,omitempty"`
	BestHeight            uint32    `json:"best_height,omitempty"`
	BestTime              time.Time `json:"best_time,omitempty"`
	BestRatio             float64   `json:"best_ratio,omitempty"`
	BestRatioShareDiff    float64   `json:"best_ratio_share_diff,omitempty"`
	BestRatioNetDiff      float64   `json:"best_ratio_net_diff,omitempty"`
	BestRatioHeight       uint32    `json:"best_ratio_height,omitempty"`
	BestRatioTime         time.Time `json:"best_ratio_time,omitempty"`
	Hashrate1m            float64   `json:"hashrate_1m"`
	Hashrate5m            float64   `json:"hashrate_5m"`
	Hashrate15m           float64   `json:"hashrate_15m"`
	Vendor                string    `json:"vendor,omitempty"`
	Firmware              string    `json:"firmware,omitempty"`
	DeviceID              string    `json:"device_id,omitempty"`
}

// MinersResponse is the JSON response for GET /miners.
type MinersResponse struct {
	Miners map[string][]MinerInfo `json:"miners"`
}

// APIServer serves metrics over HTTP.
type APIServer struct {
	port             int
	poolName         string
	stats            *Stats
	sessionProvider  SessionProvider
	server           *http.Server
	mux              *http.ServeMux
	logger           *logging.Logger
	startTime        time.Time
	metricsHandler   http.HandlerFunc
	fleetHandler     http.HandlerFunc
	poolRatioHandler http.HandlerFunc
}

func (a *APIServer) SetFleetHandler(h http.HandlerFunc) {
	a.fleetHandler = h
}

// SetPoolRatioHandler sets the callback for the pool-wide best-ratio endpoint.
func (a *APIServer) SetPoolRatioHandler(h http.HandlerFunc) {
	a.poolRatioHandler = h
}

// NewAPIServer creates a new metrics API server.
func NewAPIServer(port int, poolName string, stats *Stats) *APIServer {
	return &APIServer{
		port:      port,
		poolName:  poolName,
		stats:     stats,
		logger:    logging.New(logging.ModuleMetrics),
		startTime: time.Now(),
	}
}

func (a *APIServer) SetMetricsHandler(h http.HandlerFunc) {
	a.metricsHandler = h
}

// SetSessionProvider sets the callback for retrieving active sessions.
func (a *APIServer) SetSessionProvider(sp SessionProvider) {
	a.sessionProvider = sp
}

// Start begins serving the metrics API.
func (a *APIServer) Start() error {
	a.mux = http.NewServeMux()
	mux := a.mux

	// Existing endpoints
	mux.HandleFunc("/stats", a.handleStats)
	mux.HandleFunc("/miners", a.handleMiners)
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/metrics", a.metricsHandler)

	// 🔥 NEW: Engine Fleet API
	mux.HandleFunc("/api/engine/fleet", func(w http.ResponseWriter, r *http.Request) {
		if a.fleetHandler != nil {
			a.fleetHandler(w, r)
			return
		}
		http.Error(w, "fleet handler not set", http.StatusInternalServerError)
	})
	mux.HandleFunc("/pool-ratio", func(w http.ResponseWriter, r *http.Request) {
		if a.poolRatioHandler != nil {
			a.poolRatioHandler(w, r)
			return
		}
		http.Error(w, "pool-ratio handler not set", http.StatusInternalServerError)
	})

	// 🔥 NEW: Serve UI
	staticDir := resolveStaticDir()
	mux.HandleFunc("/disconnects", a.handleDisconnects)
	mux.HandleFunc("/disconnects/", a.handleDeleteDisconnect)
	mux.HandleFunc("/connects", a.handleConnects)
	mux.HandleFunc("/connects/", a.handleDeleteConnect)
	mux.Handle("/", serveStaticUI(staticDir))

	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", a.port),
		Handler: mux,
	}

	a.logger.Info("Metrics API listening on :%d", a.port)

	go func() {
		if err := a.server.ListenAndServe(); err != http.ErrServerClosed {
			a.logger.Error("metrics API error: %v", err)
		}
	}()

	return nil
}

func resolveStaticDir() string {
	if _, err := os.Stat("static/index.html"); err == nil {
		return "static"
	}

	if exe, err := os.Executable(); err == nil {
		dir := filepath.Join(filepath.Dir(exe), "static")
		if _, statErr := os.Stat(filepath.Join(dir, "index.html")); statErr == nil {
			return dir
		}
	}

	return "static"
}

func serveStaticUI(staticDir string) http.Handler {
	fileServer := http.FileServer(http.Dir(staticDir))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}

		requestPath := strings.TrimPrefix(r.URL.Path, "/")
		requestPath = strings.TrimPrefix(requestPath, "static/")

		if requestPath == "" {
			http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
			return
		}

		fullPath := filepath.Join(staticDir, filepath.Clean(requestPath))
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			if strings.HasPrefix(r.URL.Path, "/static/") {
				r = r.Clone(r.Context())
				r.URL.Path = "/" + requestPath
			}
			fileServer.ServeHTTP(w, r)
			return
		}

		http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
	})
}

// Stop shuts down the API server.
func (a *APIServer) Mux() *http.ServeMux {
	if a.mux == nil {
		a.mux = http.NewServeMux()
	}
	return a.mux
}

func (a *APIServer) Stop() {
	if a.server != nil {
		a.server.Close()
	}
	a.logger.Info("Metrics API stopped")
}

func (a *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := PoolStatsResponse{
		PoolName:      a.poolName,
		UptimeSeconds: a.stats.UptimeSeconds(),
		Coins:         a.stats.GetAllStats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *APIServer) handleMiners(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := MinersResponse{
		Miners: make(map[string][]MinerInfo),
	}

	if a.sessionProvider == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	allSessions := a.sessionProvider()
	for symbol, sessions := range allSessions {
		workerStats := a.stats.GetWorkerStats(symbol)
		miners := make([]MinerInfo, 0, len(sessions))

		for _, sess := range sessions {
			mi := MinerInfo{
				WorkerName:  sess.WorkerName,
				RemoteAddr:  sess.RemoteAddr,
				Difficulty:  sess.Difficulty,
				ConnectedAt: sess.ConnectedAt,
				Vendor:      sess.Vendor,
				Firmware:    sess.Firmware,
				DeviceID:    sess.DeviceID,
			}
			// Best-share context and best-ratio come from the session (channel).
			mi.BestNetworkDiff = sess.BestNetworkDiff
			mi.BestHeight = sess.BestHeight
			mi.BestTime = sess.BestTime
			mi.BestRatio = sess.BestRatio
			mi.BestRatioShareDiff = sess.BestRatioShareDiff
			mi.BestRatioNetDiff = sess.BestRatioNetDiff
			mi.BestRatioHeight = sess.BestRatioHeight
			mi.BestRatioTime = sess.BestRatioTime
			if ws, ok := workerStats[sess.WorkerName]; ok {
				ws.mu.Lock()
				mi.SharesAccepted = ws.SharesAccepted
				mi.SharesRejected = ws.SharesRejected
				mi.SessionSharesAccepted = sess.SharesAccepted
				mi.SessionSharesRejected = sess.SharesRejected
				mi.Protocol = sess.Protocol
				mi.BestDifficultySession = sess.BestDifficulty
				if !ws.LastSeenTime.IsZero() {
					mi.LastSeenTime = ws.LastSeenTime.UTC().Format(time.RFC3339Nano)
				}
				mi.SharesStale = ws.SharesStale
				mi.BlocksFound = ws.BlocksFound
				mi.LastShareTime = ws.LastShareTime
				mi.BestDifficulty = ws.BestDifficulty
				mi.Hashrate1m = ws.HashrateAt(1 * time.Minute)
				mi.Hashrate5m = ws.HashrateAt(5 * time.Minute)
				mi.Hashrate15m = ws.HashrateAt(15 * time.Minute)
				ws.mu.Unlock()
			}
			miners = append(miners, mi)
		}
		resp.Miners[symbol] = miners
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *APIServer) handleDisconnects(w http.ResponseWriter, r *http.Request) {
	disconnects := a.stats.RecentDisconnects()
	type disconnectEntry struct {
		DisconnectTime string `json:"disconnect_time"`
		ConnectedAt    string `json:"connected_at"`
	}
	result := make(map[string]map[string]disconnectEntry)
	for symbol, workers := range disconnects {
		result[symbol] = make(map[string]disconnectEntry)
		for name, info := range workers {
			result[symbol][name] = disconnectEntry{
				DisconnectTime: info.DisconnectTime.UTC().Format(time.RFC3339Nano),
				ConnectedAt:    info.ConnectedAt.UTC().Format(time.RFC3339Nano),
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (a *APIServer) handleDeleteDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/disconnects/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "usage: DELETE /disconnects/{symbol}/{worker}", http.StatusBadRequest)
		return
	}
	symbol, workerName := parts[0], parts[1]
	a.stats.DeleteDisconnect(symbol, workerName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *APIServer) handleConnects(w http.ResponseWriter, r *http.Request) {
	connects := a.stats.RecentConnects()
	result := make(map[string]map[string]string)
	for symbol, workers := range connects {
		result[symbol] = make(map[string]string)
		for name, t := range workers {
			result[symbol][name] = t.UTC().Format(time.RFC3339Nano)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (a *APIServer) handleDeleteConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/connects/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "usage: DELETE /connects/{symbol}/{worker}", http.StatusBadRequest)
		return
	}
	symbol, workerName := parts[0], parts[1]
	a.stats.DeleteConnect(symbol, workerName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *APIServer) handleMetrics(w http.ResponseWriter, r *http.Request) {

	miners := 0
	if a.sessionProvider != nil {
		sessions := a.sessionProvider()
		for _, list := range sessions {
			miners += len(list)
		}
	}

	response := map[string]interface{}{
		"pool_name":        a.poolName,
		"uptime_seconds":   time.Since(a.startTime).Seconds(),
		"miners_connected": miners,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
