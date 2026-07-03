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
	"sync"
	"time"
)

// ShareResult represents the outcome of a share submission.
type ShareResult int

const (
	ShareValid ShareResult = iota
	ShareInvalid
	ShareStale
)

// WorkerStats holds per-worker statistics.
type WorkerStats struct {
	SharesAccepted uint64    `json:"shares_accepted"`
	SharesRejected uint64    `json:"shares_rejected"`
	SharesStale    uint64    `json:"shares_stale"`
	BlocksFound    uint64    `json:"blocks_found"`
	LastShareTime  time.Time `json:"last_share_time,omitempty"`
	BestDifficulty float64   `json:"best_difficulty"`
	recentShares   []workerShareSample
	mu             sync.Mutex
}

// workerShareSample is a timestamped share difficulty sample for hashrate calculation.
type workerShareSample struct {
	t    time.Time
	diff float64
}

// HashrateAt returns the estimated hashrate (TH/s) over the given window.
func (ws *WorkerStats) HashrateAt(window time.Duration) float64 {
	cutoff := time.Now().Add(-window)
	var diffSum float64
	for _, s := range ws.recentShares {
		if s.t.After(cutoff) {
			diffSum += s.diff
		}
	}
	if diffSum == 0 {
		return 0
	}
	return (diffSum * 4294967296) / window.Seconds() / 1e12
}

// pruneShares removes samples older than 15 minutes.
func (ws *WorkerStats) pruneShares() {
	cutoff := time.Now().Add(-15 * time.Minute)
	filtered := ws.recentShares[:0]
	for _, s := range ws.recentShares {
		if s.t.After(cutoff) {
			filtered = append(filtered, s)
		}
	}
	ws.recentShares = filtered
}

// CoinStats holds per-coin statistics.
type CoinStats struct {
	SharesAccepted  uint64    `json:"shares_accepted"`
	SharesRejected  uint64    `json:"shares_rejected"`
	SharesStale     uint64    `json:"shares_stale"`
	BlocksFound     uint64    `json:"blocks_found"`
	LastBlockHash   string    `json:"last_block_hash,omitempty"`
	LastBlockHeight int64     `json:"last_block_height,omitempty"`
	LastBlockTime   time.Time `json:"last_block_time,omitempty"`
	SyncProgress    float64   `json:"sync_progress"`
	MaxPoolHashrate float64   `json:"max_pool_hashrate"`
	mu              sync.Mutex
}

// Stats holds in-memory statistics for all coins.
type Stats struct {
	coins     map[string]*CoinStats
	workers   map[string]map[string]*WorkerStats // coin symbol -> worker name -> stats
	mu        sync.RWMutex
	startedAt time.Time
}

// Stats new section for sync status and percentage.
// UpdateMaxPoolHashrate updates the peak pool hashrate if current exceeds stored max.
func (s *Stats) UpdateMaxPoolHashrate(symbol string, current float64) {
	s.mu.RLock()
	coin, ok := s.coins[symbol]
	s.mu.RUnlock()
	if !ok {
		return
	}
	coin.mu.Lock()
	if current > coin.MaxPoolHashrate {
		coin.MaxPoolHashrate = current
	}
	coin.mu.Unlock()
}

func (s *Stats) SetSyncProgress(symbol string, progress float64) {
	s.mu.RLock()
	coin, ok := s.coins[symbol]
	s.mu.RUnlock()

	if !ok {
		return
	}

	coin.mu.Lock()
	coin.SyncProgress = progress
	coin.mu.Unlock()
}

func (s *Stats) GetCoinsSnapshot() map[string]CoinStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[string]CoinStats)

	for symbol, coin := range s.coins {
		coin.mu.Lock()
		snapshot[symbol] = *coin
		coin.mu.Unlock()
	}

	return snapshot
}

// NewStats creates a new Stats instance.
func NewStats() *Stats {
	return &Stats{
		coins:     make(map[string]*CoinStats),
		workers:   make(map[string]map[string]*WorkerStats),
		startedAt: time.Now(),
	}
}

// InitCoin initializes stats tracking for a coin symbol.
func (s *Stats) InitCoin(symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.coins[symbol]; !exists {
		s.coins[symbol] = &CoinStats{}
	}
	if _, exists := s.workers[symbol]; !exists {
		s.workers[symbol] = make(map[string]*WorkerStats)
	}
}

// RecordShare records a share submission result.
func (s *Stats) RecordShare(symbol string, result ShareResult, workerName string, shareDiff float64) {
	s.mu.RLock()
	cs, ok := s.coins[symbol]
	s.mu.RUnlock()
	if !ok {
		return
	}

	cs.mu.Lock()
	switch result {
	case ShareValid:
		cs.SharesAccepted++
	case ShareInvalid:
		cs.SharesRejected++
	case ShareStale:
		cs.SharesStale++
	}
	cs.mu.Unlock()

	// Track per-worker stats
	if workerName != "" {
		ws := s.getOrCreateWorker(symbol, workerName)
		ws.mu.Lock()
		switch result {
		case ShareValid:
			ws.SharesAccepted++
		case ShareInvalid:
			ws.SharesRejected++
		case ShareStale:
			ws.SharesStale++
		}
		ws.LastShareTime = time.Now()
		bestDiff := shareDiff
		if len(actualDiff) > 0 && actualDiff[0] > 0 {
			bestDiff = actualDiff[0]
		}
		if bestDiff > ws.BestDifficulty {
			ws.BestDifficulty = bestDiff
		}
		if shareDiff > 0 {
			ws.recentShares = append(ws.recentShares, workerShareSample{t: time.Now(), diff: shareDiff})
			ws.pruneShares()
		}
		ws.mu.Unlock()
	}
}

// RecordBlock records a block found event.
func (s *Stats) RecordBlock(symbol, blockHash string, height int64, workerName string) {
	s.mu.RLock()
	cs, ok := s.coins[symbol]
	s.mu.RUnlock()
	if !ok {
		return
	}

	cs.mu.Lock()
	cs.BlocksFound++
	cs.LastBlockHash = blockHash
	cs.LastBlockHeight = height
	cs.LastBlockTime = time.Now()
	cs.mu.Unlock()

	if workerName != "" {
		ws := s.getOrCreateWorker(symbol, workerName)
		ws.mu.Lock()
		ws.BlocksFound++
		ws.mu.Unlock()
	}
}

// getOrCreateWorker returns the WorkerStats for a worker, creating it if needed.
func (s *Stats) getOrCreateWorker(symbol, workerName string) *WorkerStats {
	s.mu.RLock()
	workers, ok := s.workers[symbol]
	if ok {
		if ws, exists := workers[workerName]; exists {
			s.mu.RUnlock()
			return ws
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers[symbol] == nil {
		s.workers[symbol] = make(map[string]*WorkerStats)
	}
	if ws, exists := s.workers[symbol][workerName]; exists {
		return ws
	}
	ws := &WorkerStats{}
	s.workers[symbol][workerName] = ws
	return ws
}

// GetCoinStats returns a snapshot of stats for a specific coin.
func (s *Stats) GetCoinStats(symbol string) *CoinStats {
	s.mu.RLock()
	cs, ok := s.coins[symbol]
	s.mu.RUnlock()
	if !ok {
		return nil
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Return a copy
	return &CoinStats{
		SharesAccepted:  cs.SharesAccepted,
		SharesRejected:  cs.SharesRejected,
		SharesStale:     cs.SharesStale,
		BlocksFound:     cs.BlocksFound,
		LastBlockHash:   cs.LastBlockHash,
		LastBlockHeight: cs.LastBlockHeight,
		LastBlockTime:   cs.LastBlockTime,
	}
}

// GetAllStats returns a snapshot of stats for all coins.
func (s *Stats) GetAllStats() map[string]*CoinStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*CoinStats)
	for symbol, cs := range s.coins {
		cs.mu.Lock()
		result[symbol] = &CoinStats{
			SharesAccepted:  cs.SharesAccepted,
			SharesRejected:  cs.SharesRejected,
			SharesStale:     cs.SharesStale,
			BlocksFound:     cs.BlocksFound,
			LastBlockHash:   cs.LastBlockHash,
			LastBlockHeight: cs.LastBlockHeight,
			LastBlockTime:   cs.LastBlockTime,
		}
		cs.mu.Unlock()
	}
	return result
}

// GetWorkerStats returns a snapshot of per-worker stats for a coin.
func (s *Stats) GetWorkerStats(symbol string) map[string]*WorkerStats {
	s.mu.RLock()
	workers, ok := s.workers[symbol]
	if !ok {
		s.mu.RUnlock()
		return nil
	}

	result := make(map[string]*WorkerStats, len(workers))
	for name, ws := range workers {
		ws.mu.Lock()
		result[name] = &WorkerStats{
			SharesAccepted: ws.SharesAccepted,
			SharesRejected: ws.SharesRejected,
			SharesStale:    ws.SharesStale,
			BlocksFound:    ws.BlocksFound,
			LastShareTime:  ws.LastShareTime,
			BestDifficulty: ws.BestDifficulty,
			recentShares:   append([]workerShareSample(nil), ws.recentShares...),
		}
		ws.mu.Unlock()
	}
	s.mu.RUnlock()
	return result
}

// UptimeSeconds returns the uptime in seconds.
func (s *Stats) UptimeSeconds() float64 {
	return time.Since(s.startedAt).Seconds()
}
