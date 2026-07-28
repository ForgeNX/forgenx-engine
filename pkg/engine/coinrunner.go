/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package engine

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/coin"
	"github.com/ForgeNX/forgenx-engine/pkg/coinbase"
	"github.com/ForgeNX/forgenx-engine/pkg/config"
	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/ForgeNX/forgenx-engine/pkg/metrics"
	"github.com/ForgeNX/forgenx-engine/pkg/noderpc"
	"github.com/ForgeNX/forgenx-engine/pkg/stratum"
	"github.com/ForgeNX/forgenx-engine/pkg/stratumv2"
)

//go:embed AUTHORS
var authorsFile string

type shareWork struct {
	t    time.Time
	diff float64
}

// CoinRunner manages the complete mining pipeline for a single coin.
// workerDiffStore is a minimal interface for persisting worker difficulty.
type workerDiffStore interface {
	SaveWorkerDifficulty(symbol, workerName string, difficulty float64)
	GetWorkerLastDifficulty(symbol, workerName string) float64
}

type CoinRunner struct {
	symbol          string
	coin            coin.Coin
	store           workerDiffStore
	rpcClient       *noderpc.Client
	jobMgr          *JobManager
	validator       *ShareValidator
	server          *stratum.Server
	sv2Server       *stratumv2.Server
	zmqSub          *noderpc.ZMQSubscriber
	stats           *metrics.Stats
	logger          *logging.Logger
	acceptedShares  int64
	totalDifficulty float64
	startTime       time.Time
	recentShares    []shareWork
	sharesMu        sync.Mutex // protects acceptedShares + recentShares + bestRatio*
	// lastBlockSubmit is the time of the most recent block submission, shared
	// across V1 and V2 so the eCash cooldown coordinates between both protocols.
	lastBlockSubmit   time.Time
	lastBlockSubmitMu sync.Mutex

	// Pool-wide best-ratio (engine-session): the closest ANY worker has come
	// to a block since this runner started. Resets only on engine restart.
	bestRatio          float64
	bestRatioShareDiff float64
	bestRatioNetDiff   float64
	bestRatioHeight    uint32
	bestRatioTime      time.Time
	bestRatioWorker    string
}

// NewCoinRunner creates and wires up all components for a single coin.
func (r *CoinRunner) SetStore(s workerDiffStore) { r.store = s }

// BestRatioContext returns the pool-wide (engine-session) best shareDiff/
// networkDiff ratio for this coin — the closest any worker has come to a block
// since the runner started — plus the share behind it and the worker name.
func (r *CoinRunner) BestRatioContext() (ratio, shareDiff, netDiff float64, height uint32, when time.Time, worker string) {
	r.sharesMu.Lock()
	defer r.sharesMu.Unlock()
	return r.bestRatio, r.bestRatioShareDiff, r.bestRatioNetDiff, r.bestRatioHeight, r.bestRatioTime, r.bestRatioWorker
}

func NewCoinRunner(symbol string, cfg config.CoinConfig, donation config.DonationConfig, stats *metrics.Stats) (*CoinRunner, error) {
	soloMode := cfg.Mining.Mode == "solo"

	// Register generic coin from definition if coin_type is not built-in
	if _, err := coin.Get(cfg.CoinType); err != nil && cfg.CoinDefinition != nil {
		coin.Register(cfg.CoinType, coin.NewGenericCoin(cfg.CoinType, *cfg.CoinDefinition))
	}

	// Look up coin implementation
	c, err := coin.Get(cfg.CoinType)
	if err != nil {
		return nil, fmt.Errorf("coin type %s: %w", cfg.CoinType, err)
	}

	// Validate mining address (required in pool mode, optional in solo mode)
	if !soloMode {
		if err := c.ValidateAddress(cfg.Mining.Address, cfg.Mining.Network); err != nil {
			return nil, fmt.Errorf("invalid mining address for %s: %w", symbol, err)
		}
	}

	// Create RPC client
	rpcClient := noderpc.NewClient(
		cfg.Node.Host, cfg.Node.Port,
		cfg.Node.Username, cfg.Node.Password,
	)

	runner := &CoinRunner{
		symbol:    symbol,
		coin:      c,
		rpcClient: rpcClient,
		stats:     stats,
		logger:    logging.New(logging.ModuleEngine),
		startTime: time.Now(),
	}

	// 🔥 CRITICAL: Register coin in stats immediately
	stats.InitCoin(symbol)

	// Create stratum server
	var vardiffCfg *stratum.VarDiffConfig
	if cfg.VarDiff.Enabled {
		vardiffCfg = &stratum.VarDiffConfig{
			MinDiff:           cfg.VarDiff.MinDiff,
			MaxDiff:           cfg.VarDiff.MaxDiff,
			TargetTime:        cfg.VarDiff.TargetTime,
			RetargetTime:      cfg.VarDiff.RetargetTime,
			VariancePct:       cfg.VarDiff.VariancePct,
			FloatDiff:         cfg.VarDiff.FloatDiff,
			FloatDiffBelowOne: cfg.VarDiff.FloatDiffBelowOne != nil && *cfg.VarDiff.FloatDiffBelowOne,
			FloatPrecision:    cfg.VarDiff.FloatPrecision,
		}
	}

	// Apply coin-level donation overrides before resolving scripts
	if cfg.Donation != nil {
		if !cfg.Donation.Enabled {
			donation.Enabled = false
		}
		if cfg.Donation.Percent > 0 {
			donation.Percent = cfg.Donation.Percent
		}
		if cfg.Donation.Enabled2 {
			donation.Enabled2 = true
		}
		if cfg.Donation.Address2 != "" {
			donation.Address2 = cfg.Donation.Address2
		}
		if cfg.Donation.Percent2 > 0 {
			donation.Percent2 = cfg.Donation.Percent2
		}
	}
	// Resolve donation output script from AUTHORS file
	var donationScript []byte
	var donationPercent float64
	if donation.Enabled && donation.Percent > 0 {
		if addr, err := loadDonationAddress(symbol, cfg.Mining.Network); err != nil {
			runner.logger.Warn("[%s] donation disabled: %v", symbol, err)
		} else if script, err := c.AddressToScript(addr, cfg.Mining.Network); err != nil {
			runner.logger.Warn("[%s] donation disabled: invalid address %s: %v", symbol, addr, err)
		} else {
			donationScript = script
			donationPercent = donation.Percent
			runner.logger.Info("[%s] donation enabled: %.1f%% to %s", symbol, donationPercent, addr)
		}
	}

	// Merge coin-level donation settings over global
	if cfg.Donation != nil {
		if cfg.Donation.Enabled2 {
			donation.Enabled2 = true
		}
		if cfg.Donation.Address2 != "" {
			donation.Address2 = cfg.Donation.Address2
		}
		if cfg.Donation.Percent2 > 0 {
			donation.Percent2 = cfg.Donation.Percent2
		}
		// Also allow coin-level override of donation1
		if !cfg.Donation.Enabled {
			donation.Enabled = false
		}
		if cfg.Donation.Percent > 0 {
			donation.Percent = cfg.Donation.Percent
		}
	}
	// Resolve custom donation2 address
	var donation2Script []byte
	var donation2Percent float64
	if donation.Enabled2 && donation.Percent2 > 0 && donation.Address2 != "" {
		script2, err2 := c.AddressToScript(donation.Address2, cfg.Mining.Network)
		if err2 != nil {
			runner.logger.Warn("[%s] donation2 disabled: invalid address %s: %v", symbol, donation.Address2, err2)
		} else {
			donation2Script = script2
			donation2Percent = donation.Percent2
			runner.logger.Info("[%s] donation2 enabled: %.1f%% to %s", symbol, donation2Percent, donation.Address2)
		}
	}

	// Create job manager
	jobMgr := NewJobManager(JobManagerConfig{
		Coin:           c,
		RPCClient:      rpcClient,
		Address:        cfg.Mining.Address,
		Network:        cfg.Mining.Network,
		CoinbaseText:   cfg.Mining.CoinbaseText,
		ExtraNonceSize: cfg.Mining.ExtraNonceSize,
		PollInterval: func() time.Duration {
			if cfg.TemplateRefreshInterval <= 0 {
				return 5 * time.Second
			}
			return time.Duration(cfg.TemplateRefreshInterval) * time.Second
		}(),
		SoloMode:         soloMode,
		DonationScript:   donationScript,
		DonationPercent:  donationPercent,
		Donation2Script:  donation2Script,
		Donation2Percent: donation2Percent,
	})
	runner.jobMgr = jobMgr

	// Create share validator
	staleGrace := time.Duration(cfg.Stratum.StaleShareGrace) * time.Second
	lowDiffGrace := time.Duration(cfg.Stratum.LowDiffShareGrace) * time.Second
	validator := NewShareValidator(c, jobMgr, rpcClient, stats, runner, soloMode, staleGrace, lowDiffGrace)
	runner.validator = validator

	// Wire share handler: stratum server calls validator
	shareHandler := func(session *stratum.Session, share *stratum.ShareSubmission) error {
		return validator.ValidateShare(session, share)
	}

	// Build server config
	serverCfg := stratum.ServerConfig{
		Host:              cfg.Stratum.Host,
		Port:              cfg.Stratum.Port,
		ExtraNonceSize:    cfg.Mining.ExtraNonceSize,
		DefaultDiff:       cfg.Stratum.Difficulty,
		AcceptSuggestDiff: cfg.Stratum.AcceptSuggestDiff,
		PingEnabled:       cfg.Stratum.PingEnabled,
		PingInterval:      time.Duration(cfg.Stratum.PingInterval) * time.Second,
		IdleTimeout:       5 * time.Minute,
		VarDiff:           vardiffCfg,
		VarDiffOnNewBlock: cfg.VarDiff.OnNewBlock == nil || *cfg.VarDiff.OnNewBlock,
	}

	// Solo mode: set up authorize, job-for-session, and disconnect handlers
	if soloMode {
		serverCfg.AuthorizeHandler = func(session *stratum.Session, workerName string) (string, error) {
			// Parse address from workerName: "address.workerID" -> "address"
			address := workerName
			if dotIdx := strings.Index(workerName, "."); dotIdx > 0 {
				address = workerName[:dotIdx]
			}

			// Validate the address
			if err := c.ValidateAddress(address, cfg.Mining.Network); err != nil {
				return "", fmt.Errorf("invalid mining address: %w", err)
			}

			// Register this address with the job manager
			jobMgr.RegisterAddress(address)

			return address, nil
		}

		serverCfg.JobForSessionHandler = func(session *stratum.Session) *stratum.Job {
			return jobMgr.GetJobForAddress(session.MiningAddress())
		}

		serverCfg.OnSessionAuthorized = func(session *stratum.Session) {
			if wn := session.WorkerName(); wn != "" {
				runner.stats.RecordConnect(runner.symbol, wn)
				if runner.store != nil {
					if lastDiff := runner.store.GetWorkerLastDifficulty(runner.symbol, wn); lastDiff > 0 {
						minD := cfg.VarDiff.MinDiff
						maxD := cfg.VarDiff.MaxDiff
						if lastDiff < minD {
							lastDiff = minD
						}
						if maxD > 0 && lastDiff > maxD {
							lastDiff = maxD
						}
						session.SetInitialDifficulty(lastDiff)
					}
				}
			}
		}
		serverCfg.OnSessionRemoved = func(session *stratum.Session) {
			if addr := session.MiningAddress(); addr != "" {
				jobMgr.UnregisterAddress(addr)
			}
			if wn := session.WorkerName(); wn != "" {
				runner.stats.RecordDisconnect(runner.symbol, wn, session.ConnectedAt())
				runner.store.SaveWorkerDifficulty(runner.symbol, wn, session.GetDifficulty())
			}
		}
	}

	server := stratum.NewServer(serverCfg, shareHandler)
	runner.server = server

	// Wire job manager broadcast
	if soloMode {
		jobMgr.onNewJob = func(job *stratum.Job) {
			server.BroadcastJobPerSession(job, func(session *stratum.Session) *stratum.Job {
				addr := session.MiningAddress()
				if addr == "" {
					return nil
				}
				coinb2, ok := jobMgr.GetAddressCoinb2(job.JobID, addr)
				if !ok {
					return nil
				}
				return &stratum.Job{
					JobID:          job.JobID,
					PrevHash:       job.PrevHash,
					Coinb1:         job.Coinb1,
					Coinb2:         coinb2,
					MerkleBranches: job.MerkleBranches,
					Version:        job.Version,
					NBits:          job.NBits,
					NTime:          job.NTime,
					CleanJobs:      job.CleanJobs,
				}
			})
		}
	} else {
		jobMgr.onNewJob = func(job *stratum.Job) {
			server.BroadcastJob(job)
		}
	}

	// Set up ZMQ if enabled
	if cfg.Node.ZMQEnabled && cfg.Node.ZMQHashBlock != "" {
		runner.zmqSub = noderpc.NewZMQSubscriber(cfg.Node.ZMQHashBlock, func(blockHash string) {
			jobMgr.OnBlockNotification(blockHash)
		})
	}

	// Set up SV2 server on its own explicit, independently-configured
	// port (NOT derived from V1's port — see config.go's SV2Port field
	// and applyDefaults for why: a Port+1 offset collides with other
	// coins' V1 stratum ports once more than one coin is configured).
	if !cfg.Stratum.SV2Enabled {
		runner.logger.Info("[%s] SV2 disabled (sv2_enabled=false)", symbol)
	} else {
		sv2KP, err := loadOrGenerateSV2Key(symbol)
		if err != nil {
			runner.logger.Warn("[%s] SV2 disabled: %v", symbol, err)
		} else if sv2AuthKP, authErr := loadOrGenerateAuthorityKey(symbol); authErr != nil {
			runner.logger.Warn("[%s] SV2 disabled: authority key: %v", symbol, authErr)
		} else {
			sv2Cfg := stratumv2.Config{
				ListenAddr:               fmt.Sprintf("%s:%d", cfg.Stratum.Host, cfg.Stratum.SV2Port),
				StaticKeypair:            sv2KP,
				AuthorityKeypair:         sv2AuthKP,
				CoinTicker:               symbol,
				VarDiff:                  vardiffCfg,
				VarDiffOnNewBlock:        cfg.VarDiff.OnNewBlock == nil || *cfg.VarDiff.OnNewBlock,
				StartDiff:                cfg.Stratum.Difficulty,
				Logger:                   runner.logger,
				ConnectionTimeoutSeconds: cfg.Stratum.ConnectionTimeout,
				OnShare: func(job *stratumv2.JobTemplate, ch *stratumv2.Channel, share *stratumv2.MsgSubmitSharesStandardFields, extranonce []byte, result *stratumv2.ShareResult) {
					// BLOCK SUBMISSION FIRST — before any stats/lock bookkeeping.
					// Solo mining is a latency race against the whole network, so the
					// instant we believe we have a block we dispatch it; everything
					// else (hashrate stats, best-ratio) is local bookkeeping that can
					// wait a few microseconds and must NOT sit ahead of the submit
					// (notably the sharesMu lock, which other goroutines can hold).
					if result.MeetsBlock {
						runner.logger.Info("[%s] *** SV2 BLOCK CANDIDATE FOUND *** worker=%q height=%d hash=%s",
							symbol, ch.UserIdentity(), job.Height, result.HashHex)
						// Block submission can retry with backoff (~seconds); run it
						// off the share-response path so the miner isn't blocked.
						go submitSV2Block(runner, c, rpcClient, jobMgr, job, ch, share, extranonce, result, symbol)
					}
					// Use pool difficulty (not actual hash difficulty) for hashrate estimation.
					// Actual hash diff can be astronomically high (lucky shares) and would
					// massively skew rolling hashrate averages — same approach as V1.
					poolDiff := ch.Difficulty()
					// Record in unified stats so SV2 workers appear in /miners and UI Workers tab.
					runner.stats.RecordShare(symbol, metrics.ShareValid, ch.UserIdentity(), poolDiff, result.Difficulty)
					// Network difficulty of this share's job, derived the same way as
					// the block check, for the pool-wide best-ratio tracking below.
					netDiff := stratumv2.TargetToDifficulty(stratumv2.NBitsToTarget(job.NBits))
					// Append to recentShares (hashrate) and update pool-wide best-ratio
					// under one lock. Best-ratio = closest any worker has come to a
					// block this engine-session (distinct from best difficulty).
					runner.sharesMu.Lock()
					runner.recentShares = append(runner.recentShares, shareWork{t: time.Now(), diff: poolDiff})
					if netDiff > 0 {
						if ratio := result.Difficulty / netDiff; ratio > runner.bestRatio {
							runner.bestRatio = ratio
							runner.bestRatioShareDiff = result.Difficulty
							runner.bestRatioNetDiff = netDiff
							runner.bestRatioHeight = job.Height
							runner.bestRatioTime = time.Now().UTC()
							runner.bestRatioWorker = ch.UserIdentity()
						}
					}
					runner.sharesMu.Unlock()
				},
				OnStale: func(workerName string) {
					runner.stats.RecordShare(symbol, metrics.ShareStale, workerName, 0)
				},
				OnRejected: func(workerName string) {
					runner.stats.RecordShare(symbol, metrics.ShareInvalid, workerName, 0)
				},
				LowDiffGrace: time.Duration(cfg.Stratum.LowDiffShareGrace) * time.Second,
				StaleGrace:   time.Duration(cfg.Stratum.StaleShareGrace) * time.Second,
				TipChangedAt: jobMgr.TipChangedAt,
			}

			// Solo mode: each channel gets its own coinbase, built fresh
			// against the JobManager's latest template, paying out to the
			// channel's own UserIdentity address — exactly mirroring V1's
			// AuthorizeHandler + AddressCoinb2s pattern, just resolved
			// per-SV2-channel instead of precomputed for known sessions.
			if soloMode {
				sv2Cfg.CoinbaseBuilder = func(userIdentity string) (coinb1, coinb2 []byte, err error) {
					// Parse address from userIdentity: "address.workerID" -> "address"
					// Same convention as the V1 AuthorizeHandler above.
					address := userIdentity
					if dotIdx := strings.Index(userIdentity, "."); dotIdx > 0 {
						address = userIdentity[:dotIdx]
					}

					if err := c.ValidateAddress(address, cfg.Mining.Network); err != nil {
						return nil, nil, fmt.Errorf("invalid sv2 worker address %q: %w", address, err)
					}

					template := jobMgr.LatestTemplate()
					if template == nil {
						return nil, nil, fmt.Errorf("no block template available yet")
					}

					c1Hex, c2Hex, err := c.BuildCoinbase(
						template, address, cfg.Mining.Network, cfg.Mining.CoinbaseText,
						jobMgr.ExtraNonce1Size(), jobMgr.ExtraNonce2Size(),
						jobMgr.donationOutputs(template),
					)
					if err != nil {
						return nil, nil, fmt.Errorf("building sv2 coinbase for %s: %w", address, err)
					}

					cb1, err := hex.DecodeString(c1Hex)
					if err != nil {
						return nil, nil, fmt.Errorf("decode coinb1: %w", err)
					}
					cb2, err := hex.DecodeString(c2Hex)
					if err != nil {
						return nil, nil, fmt.Errorf("decode coinb2: %w", err)
					}
					return cb1, cb2, nil
				}

				// Register the SV2 worker's address with the job manager the same
				// way V1's AuthorizeHandler does, so it shows up in connectedAddrs
				// for stats/accounting parity with V1 solo workers. This is fired
				// from the OpenStandardMiningChannel handler indirectly via the
				// CoinbaseBuilder call above on first build; explicit registration
				// here is intentionally omitted to avoid double-counting against
				// jobMgr.connectedAddrs, which currently assumes V1 session
				// lifecycle (Authorize/disconnect) for its ref-counting. SV2
				// worker accounting is tracked independently via
				// sv2Server.Stats() / Channel.Stats() — see metrics wiring in a
				// follow-up patch if unified accounting is needed later.
			}

			sv2Cfg.OnConnect = func(workerName, remoteAddr string) {
				if workerName != "" {
					runner.stats.RecordConnect(runner.symbol, workerName)
				}
			}
			sv2Cfg.StartDiffFunc = func(workerName string) float64 {
				if runner.store == nil {
					return cfg.Stratum.Difficulty
				}
				lastDiff := runner.store.GetWorkerLastDifficulty(runner.symbol, workerName)
				if lastDiff <= 0 {
					return cfg.Stratum.Difficulty
				}
				minD := cfg.VarDiff.MinDiff
				maxD := cfg.VarDiff.MaxDiff
				if lastDiff < minD {
					lastDiff = minD
				}
				if maxD > 0 && lastDiff > maxD {
					lastDiff = maxD
				}
				return lastDiff
			}
			sv2Cfg.OnDisconnect = func(workerName, remoteAddr string, connectedAt time.Time) {
				if workerName != "" {
					runner.stats.RecordDisconnect(runner.symbol, workerName, connectedAt)
				}
			}
			sv2Cfg.OnDisconnectWithDiff = func(workerName string, difficulty float64) {
				if runner.store != nil && workerName != "" && difficulty > 0 {
					runner.store.SaveWorkerDifficulty(runner.symbol, workerName, difficulty)
				}
			}
			sv2Srv, err := stratumv2.NewServer(sv2Cfg)
			if err != nil {
				runner.logger.Warn("[%s] SV2 disabled: %v", symbol, err)
			} else {
				runner.sv2Server = sv2Srv
				authPubHex := hex.EncodeToString(func() []byte { b := sv2AuthKP.XOnlyPubKeyBytes(); return b[:] }())
				runner.logger.Info("[%s] SV2 server configured on %s (solo=%v) authority_pubkey=%s",
					symbol, sv2Cfg.ListenAddr, soloMode, authPubHex)
				persistSV2AuthPubkey(symbol, authPubHex, runner.logger)
			}
		}
	} // closes "if !cfg.Stratum.SV2Enabled { ... } else { ... }"

	// Wire SV2 template broadcast alongside the existing V1 onNewJob.
	// Fires from the exact same refreshTemplate() call as V1 — same
	// template, same instant, no separate polling or staleness skew.
	//
	// Note: in solo mode, evt.JobData.Coinb1/Coinb2 here are the POOL
	// FALLBACK coinbase (cfg.Mining.Address) — this is fine, because
	// sendJobToChannel/handleSubmitShares in pkg/stratumv2 prefer each
	// channel's own coinbase (set via CoinbaseBuilder above) whenever
	// one is present, falling back to this shared one only before a
	// channel's first successful coinbase build completes.
	jobMgr.onNewJobV2 = func(evt NewJobEvent) {
		if runner.sv2Server == nil {
			return
		}
		src := stratumv2.V1JobSource{
			JobIDHex:          evt.JobData.Job.JobID,
			PrevBlockHashHex:  evt.Template.PreviousBlockHash,
			Coinb1Hex:         evt.JobData.Coinb1,
			Coinb2Hex:         evt.JobData.Coinb2,
			MerkleBranchesHex: evt.JobData.Job.MerkleBranches,
			VersionHex:        evt.JobData.Job.Version,
			NBitsHex:          evt.JobData.Job.NBits,
			NTimeHex:          evt.JobData.Job.NTime,
			Height:            uint32(evt.Template.Height),
			CleanJobs:         evt.JobData.Job.CleanJobs,
		}
		tmpl, err := stratumv2.BuildTemplateFromV1Job(src)
		if err != nil {
			runner.logger.Error("[%s] SV2 template build failed: %v", symbol, err)
			return
		}
		// Attach the exact source template so a solved block is assembled from
		// the transaction set this job was mined against, not the latest.
		tmpl.SourceTemplate = evt.Template
		runner.sv2Server.BroadcastTemplate(tmpl)
	}

	return runner, nil
}

// Start begins the coin mining pipeline.
func (cr *CoinRunner) Start() error {
	// Test RPC connection
	if err := cr.rpcClient.Ping(); err != nil {
		return fmt.Errorf("%s: cannot connect to node: %w", cr.symbol, err)
	}

	info, err := cr.rpcClient.GetBlockchainInfo()
	if err != nil {
		return fmt.Errorf("%s: getblockchaininfo: %w", cr.symbol, err)
	}
	cr.logger.Info("[%s] connected to %s node (chain: %s, height: %d)",
		cr.symbol, cr.coin.Name(), info.Chain, info.Blocks)

	// Initialize stats for this coin
	cr.stats.InitCoin(cr.symbol)

	// Start stratum server
	if err := cr.server.Start(); err != nil {
		return fmt.Errorf("%s: starting stratum: %w", cr.symbol, err)
	}

	// Start job manager (fetches first template and begins polling)
	if err := cr.jobMgr.Start(); err != nil {
		cr.server.Stop()
		return fmt.Errorf("%s: starting job manager: %w", cr.symbol, err)
	}

	// Start ZMQ subscriber if configured
	if cr.zmqSub != nil {
		if err := cr.zmqSub.Start(); err != nil {
			cr.logger.Warn("[%s] ZMQ failed to start, falling back to polling: %v", cr.symbol, err)
			cr.zmqSub = nil
		}
	}

	// Start SV2 server if configured
	if cr.sv2Server != nil {
		go func() {
			if err := cr.sv2Server.Start(); err != nil {
				cr.logger.Error("[%s] SV2 server error: %v", cr.symbol, err)
			}
		}()
	}

	// Prime the SV2 server latestTemplate so miners connecting before
	// the first ZMQ block notification get work immediately.
	if cr.sv2Server != nil {
		go func() {
			if evt := cr.jobMgr.LatestJobEvent(); evt != nil {
				src := stratumv2.V1JobSource{
					JobIDHex:          evt.JobData.Job.JobID,
					PrevBlockHashHex:  evt.Template.PreviousBlockHash,
					Coinb1Hex:         evt.JobData.Coinb1,
					Coinb2Hex:         evt.JobData.Coinb2,
					MerkleBranchesHex: evt.JobData.Job.MerkleBranches,
					VersionHex:        evt.JobData.Job.Version,
					NBitsHex:          evt.JobData.Job.NBits,
					NTimeHex:          evt.JobData.Job.NTime,
					Height:            uint32(evt.Template.Height),
					CleanJobs:         true,
				}
				if sv2Tmpl, err := stratumv2.BuildTemplateFromV1Job(src); err == nil {
					sv2Tmpl.SourceTemplate = evt.Template
					cr.sv2Server.BroadcastTemplate(sv2Tmpl)
				}
			}
		}()
	}
	cr.logger.Info("[%s] coin runner started", cr.symbol)

	// Start sync monitoring loop
	go func() {
		for {
			info, err := cr.rpcClient.GetBlockchainInfo()
			if err == nil {

				progress := info.VerificationProgress

				// clamp edge case
				if progress > 0.999 {
					progress = 1
				}

				// store sync progress
				cr.stats.SetSyncProgress(cr.symbol, progress)

				// optional logging while syncing
				if info.InitialBlockDownload {
					cr.logger.Info("[%s] syncing %.2f%%",
						cr.symbol,
						progress*100,
					)
				}
			}

			time.Sleep(10 * time.Second)
		}
	}()

	return nil
}

// Stop shuts down the coin mining pipeline.
func (cr *CoinRunner) Stop() {
	if cr.zmqSub != nil {
		cr.zmqSub.Stop()
	}
	cr.jobMgr.Stop()
	cr.server.Stop()
	if cr.sv2Server != nil {
		cr.sv2Server.Stop()
	}
	cr.logger.Info("[%s] coin runner stopped", cr.symbol)
}

// SessionCount returns the number of active miner connections.
func (cr *CoinRunner) SessionCount() int {
	return cr.server.SessionCount()
}

// Sessions returns info for all active sessions on this coin.
func (cr *CoinRunner) Sessions() []stratum.SessionInfo {
	v1 := cr.server.Sessions()
	if cr.sv2Server == nil {
		return v1
	}
	return append(v1, cr.sv2Server.Sessions()...)
}

// loadDonationAddress looks up the donation address for a coin symbol and network
// from the embedded AUTHORS file. Returns an error if no match is found.
func loadDonationAddress(symbol, network string) (string, error) {
	for _, line := range strings.Split(authorsFile, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 3 && strings.EqualFold(fields[0], symbol) && strings.EqualFold(fields[1], network) {
			return fields[2], nil
		}
	}
	return "", fmt.Errorf("no donation address for %s/%s in AUTHORS", symbol, network)
}

// loadOrGenerateSV2Key loads the coin's persistent SV2 static keypair from
// disk, generating and saving a new one if it doesn't exist yet. The key
// file path is derived from the coin symbol: /pool/coins/<symbol>_sv2.key
func loadOrGenerateSV2Key(symbol string) (*stratumv2.StaticKeypair, error) {
	keyPath := fmt.Sprintf("/pool/coins/%s_sv2.key", strings.ToLower(symbol))

	if data, err := os.ReadFile(keyPath); err == nil {
		raw, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
		if decErr == nil && len(raw) == 32 {
			return stratumv2.LoadStaticKeypair(raw)
		}
		// Fall through to regenerate if the file is malformed.
	}

	kp, err := stratumv2.GenerateStaticKeypair()
	if err != nil {
		return nil, fmt.Errorf("generate sv2 keypair: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(kp.PrivKeyBytes())), 0600); err != nil {
		return nil, fmt.Errorf("save sv2 keypair to %s: %w", keyPath, err)
	}
	return kp, nil
}

// loadOrGenerateAuthorityKey loads the coin's persistent SV2 authority
// signing keypair, generating and saving a new one if it doesn't exist yet.
//
// UNLIKE loadOrGenerateSV2Key's StaticKeypair (the per-restart Noise DH
// identity, whose EllSwift wire encoding intentionally changes every
// restart by protocol design), this authority key SHOULD remain genuinely
// stable across restarts: it signs a certificate binding the (changing)
// static key to a (fixed) operator-controlled identity, and operators are
// expected to publish/pin its X-only pubkey out of band so SV2 clients can
// verify server identity (see GSS's "Authority Pubkey" config field).
func loadOrGenerateAuthorityKey(symbol string) (*stratumv2.AuthorityKeypair, error) {
	keyPath := fmt.Sprintf("/pool/coins/%s_sv2_authority.key", strings.ToLower(symbol))

	if data, err := os.ReadFile(keyPath); err == nil {
		raw, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
		if decErr == nil && len(raw) == 32 {
			return stratumv2.LoadAuthorityKeypair(raw)
		}
		// Fall through to regenerate if the file is malformed.
	}

	kp, err := stratumv2.GenerateAuthorityKeypair()
	if err != nil {
		return nil, fmt.Errorf("generate sv2 authority keypair: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(kp.PrivKeyBytes())), 0600); err != nil {
		return nil, fmt.Errorf("save sv2 authority keypair to %s: %w", keyPath, err)
	}
	return kp, nil
}

// submitSV2Block assembles and submits a solved block found via an SV2
// share, mirroring sharevalidator.go's proven V1 block-assembly path:
// BuildHeader -> coin.BuildBlock() -> rpcClient.SubmitBlock(). No new
// block-serialization logic — reuses the same coin.Coin interface method
// V1 already uses, just fed from SV2's data shapes (stratumv2.JobTemplate /
// Channel / share fields) instead of V1's.
func submitSV2Block(
	runner *CoinRunner,
	c coin.Coin,
	rpcClient *noderpc.Client,
	jobMgr *JobManager,
	job *stratumv2.JobTemplate,
	ch *stratumv2.Channel,
	share *stratumv2.MsgSubmitSharesStandardFields,
	extranonce []byte,
	result *stratumv2.ShareResult,
	symbol string,
) {
	// Use job.Coinbase1/2 directly — these ARE the channel-specific coinbase
	// set in sendExtendedJobToChannel, and are EXACTLY what validation hashed
	// via extendedChannelMerkleRoot(). Applying an OwnCoinbase() override here
	// (as an earlier version did) risks diverging from the validated merkle
	// root, producing a block whose body doesn't match its header.
	coinb1, coinb2 := job.Coinbase1, job.Coinbase2

	// Reconstruct the EXACT coinbase the miner hashed:
	//   Coinbase1 + Extranonce1 + minerExtranonce2 + Coinbase2
	// Extended-channel miners roll their own extranonce2 (passed in as
	// `extranonce`); omitting it yields a coinbase — and thus a whole block —
	// the node rejects with "Block decode failed". Must match the coinbase
	// used by extendedChannelMerkleRoot()/HashCoinbaseTx() during validation.
	en1 := ch.Extranonce1Bytes()
	coinbaseTx := make([]byte, 0, len(coinb1)+len(en1)+len(extranonce)+len(coinb2))
	coinbaseTx = append(coinbaseTx, coinb1...)
	coinbaseTx = append(coinbaseTx, en1...)
	coinbaseTx = append(coinbaseTx, extranonce...)
	coinbaseTx = append(coinbaseTx, coinb2...)

	coinbaseHash := stratumv2.CoinbaseHashForTemplate(coinb1, en1, extranonce, coinb2)
	merkleRoot := stratumv2.ComputeMerkleRoot(coinbaseHash, job.MerkleBranch)

	header := stratumv2.BuildHeader(share.Version, job.PrevHash, merkleRoot, share.NTime, job.NBits, share.Nonce)

	// Use the EXACT template this job was mined against (attached as
	// SourceTemplate when the job was built) — NOT jobMgr.LatestTemplate(),
	// which with DGB's 15s blocks may have already advanced to a different
	// height with a different transaction set, yielding an invalid block.
	template, ok := job.SourceTemplate.(*noderpc.BlockTemplate)
	if !ok || template == nil {
		// Fallback: latest template (better than nothing), but log loudly.
		template = jobMgr.LatestTemplate()
		runner.logger.Error("[%s] SV2 block submission: SourceTemplate missing, falling back to latest (tx set may mismatch)", symbol)
	}
	if template == nil {
		runner.logger.Error("[%s] SV2 block submission: no template available", symbol)
		return
	}

	// ── eCash (XEC) safety gates ──────────────────────────────────────────────
	// Mirror the V1 path (sharevalidator.go): eCash/Avalanche needs a post-block
	// cooldown, a chain-reorg check, and RTT validation before submitting, or we
	// risk submitting on a parked/reorged chain. blockHashBE is the double-SHA256
	// header hash reversed to big-endian — identical to V1's coinbase.ReverseBytes
	// (result.Hash is the raw DoubleSHA256, matching V1's blockHash pre-reverse).
	if symbol == "XEC" {
		blockHashBE := coinbase.ReverseBytes(result.Hash[:])
		// Cooldown: suppress submissions shortly after a block (shared V1/V2 state).
		lastSubmit := runner.LastBlockSubmit()
		if !lastSubmit.IsZero() {
			if elapsed := time.Since(lastSubmit); elapsed < ecashBlockCooldown {
				runner.logger.Warn("[%s] eCash block cooldown active (%.0fs remaining) - skipping SV2 submission",
					symbol, (ecashBlockCooldown - elapsed).Seconds())
				return
			}
		}
		// Chain reorg check: verify the tip hasn't moved since the template was fetched.
		if bestHash, err := rpcClient.GetBestBlockHash(); err != nil {
			runner.logger.Warn("[%s] could not verify chain tip before SV2 submission: %v", symbol, err)
		} else if bestHash != template.PreviousBlockHash {
			runner.logger.Warn("[%s] eCash chain reorg detected: template prevhash=%s, current tip=%s - skipping SV2 submission",
				symbol, template.PreviousBlockHash, bestHash)
			return
		}
		// RTT (Real Time Target) validation.
		if template.RTT != nil {
			now := time.Now().Unix()
			if !coin.IsRTTDataValid(template) {
				runner.logger.Warn("[%s] RTT data malformed (all timestamps identical) - letting node decide on block validity", symbol)
			} else if rttValid, rttErr := coin.CheckRTTTarget(blockHashBE, template, now); rttErr != nil {
				runner.logger.Error("[%s] RTT computation error: %v - attempting SV2 submission anyway", symbol, rttErr)
			} else if !rttValid {
				runner.logger.Warn("[%s] block rejected by RTT - hash does not meet Real Time Target, not submitting", symbol)
				return
			} else {
				runner.logger.Info("[%s] RTT check passed - block meets Real Time Target", symbol)
			}
		}
	}
	blockHex, err := c.BuildBlock(header[:], coinbaseTx, template)
	if err != nil {
		runner.logger.Error("[%s] SV2 building block: %v", symbol, err)
		return
	}

	if err := rpcClient.SubmitBlock(blockHex); err != nil {
		// SubmitBlock already retried transient failures. Persist the full hex
		// so a solved-but-unsubmitted block is never lost — it can be inspected
		// or resubmitted manually. This was previously ONLY done on the V1 path;
		// the SV2 path silently lost blocks (e.g. "Block decode failed").
		runner.logger.Error("[%s] *** SV2 POSSIBLE LOST BLOCK *** height=%d worker=%q: %v",
			symbol, job.Height, ch.UserIdentity(), err)
		persistLostBlockSV2(runner, symbol, int64(job.Height), ch.UserIdentity(), "sv2-submit-failed", blockHex)
		return
	}

	runner.logger.Info("[%s] *** SV2 BLOCK SUBMITTED *** height=%d worker=%q",
		symbol, job.Height, ch.UserIdentity())
	// Record submission time for the eCash cooldown (shared V1/V2 state).
	if symbol == "XEC" {
		runner.MarkBlockSubmit(time.Now())
	}
}

// persistLostBlockSV2 writes a solved-but-unsubmitted SV2 block's full hex to
// disk so it is never lost. Mirrors ShareValidator.persistLostBlock for the V1
// path. The hex can be resubmitted manually via the node's submitblock RPC.
func persistLostBlockSV2(runner *CoinRunner, symbol string, height int64, worker, reason, blockHex string) {
	dir := "/pool/lost-blocks"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		runner.logger.Error("[%s] could not create lost-blocks dir: %v", symbol, err)
		return
	}
	ts := time.Now().UTC()
	fname := fmt.Sprintf("%s/%s-h%d-%d.json", dir, symbol, height, ts.Unix())
	rec := map[string]interface{}{
		"symbol":    symbol,
		"height":    height,
		"worker":    worker,
		"reason":    reason,
		"block_hex": blockHex,
		"found_at":  ts.Format(time.RFC3339),
		"protocol":  "sv2",
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		runner.logger.Error("[%s] could not marshal lost-block record: %v", symbol, err)
		return
	}
	if err := os.WriteFile(fname, data, 0o644); err != nil {
		runner.logger.Error("[%s] could not write lost-block record %s: %v", symbol, fname, err)
		return
	}
	runner.logger.Error("[%s] *** LOST-BLOCK RECORD WRITTEN: %s — resubmit manually if needed ***", symbol, fname)
}

// LastBlockSubmit returns the time of the most recent block submission (any
// protocol). Zero if none yet. Used for the eCash post-block cooldown.
func (r *CoinRunner) LastBlockSubmit() time.Time {
	r.lastBlockSubmitMu.Lock()
	defer r.lastBlockSubmitMu.Unlock()
	return r.lastBlockSubmit
}

// MarkBlockSubmit records the time of a block submission (any protocol).
func (r *CoinRunner) MarkBlockSubmit(t time.Time) {
	r.lastBlockSubmitMu.Lock()
	r.lastBlockSubmit = t
	r.lastBlockSubmitMu.Unlock()
}

func (r *CoinRunner) Hashrate() float64 {

	cutoff := time.Now().Add(-300 * time.Second)

	var diffSum float64
	var filtered []shareWork

	r.sharesMu.Lock()
	for _, s := range r.recentShares {
		if s.t.After(cutoff) {
			diffSum += s.diff
			filtered = append(filtered, s)
		}
	}

	// keep only recent shares
	r.recentShares = filtered
	r.sharesMu.Unlock()

	if diffSum == 0 {
		return 0
	}

	hashes := diffSum * 4294967296

	return (hashes / 300) / 1e12
}

// persistSV2AuthPubkey writes the authority public key into the coin's
// JSON config file so the ForgeNX UI can display it without needing root
// access to the key files. Called once at SV2 server startup.
// sv2AuthPubkeyToBase58 encodes a 32-byte X-only SV2 authority pubkey into
// the Base58Check format Bitaxe firmware expects for sv2_auth_pk:
//
//	Base58Check( 0x01 0x00 || 32_byte_xonly_pubkey )
func sv2AuthPubkeyToBase58(pubkeyHex string) (string, error) {
	pubBytes, err := hex.DecodeString(pubkeyHex)
	if err != nil || len(pubBytes) != 32 {
		return "", fmt.Errorf("sv2 authority pubkey: %v", err)
	}
	payload := make([]byte, 34)
	payload[0] = 0x01
	payload[1] = 0x00
	copy(payload[2:], pubBytes)
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])
	full := append(payload, h2[:4]...)
	const alpha = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	// Convert bytes to base58 using digit-by-digit division
	digits := []byte{0}
	for _, b := range full {
		carry := int(b)
		for i := 0; i < len(digits); i++ {
			carry += int(digits[i]) << 8
			digits[i] = byte(carry % 58)
			carry /= 58
		}
		for carry > 0 {
			digits = append(digits, byte(carry%58))
			carry /= 58
		}
	}
	result := make([]byte, 0, len(digits))
	for _, b := range full {
		if b != 0 {
			break
		}
		result = append(result, '1')
	}
	for i := len(digits) - 1; i >= 0; i-- {
		result = append(result, alpha[digits[i]])
	}
	return string(result), nil
}

func persistSV2AuthPubkey(symbol, authPubHex string, logger *logging.Logger) {
	configPath := fmt.Sprintf("/pool/coins/%s.json", strings.ToLower(symbol))
	data, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return
	}
	stratum, ok := cfg["stratum"].(map[string]interface{})
	if !ok {
		stratum = make(map[string]interface{})
		cfg["stratum"] = stratum
	}
	encoded, encErr := sv2AuthPubkeyToBase58(authPubHex)
	if encErr != nil {
		logger.Warn("[%s] SV2 authority pubkey encoding failed: %v", symbol, encErr)
		return
	}
	stratum["sv2_authority_pubkey"] = encoded
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(configPath, out, 0644); err != nil {
		logger.Warn("[%s] could not persist SV2 authority pubkey: %v", symbol, err)
		return
	}
}

// persistSV2AuthPubkey writes the SV2 authority public key into the coin's
// JSON config so the ForgeNX UI can display it without needing root access
// to the key files. Called once at SV2 server startup.

// StratumRunning reports whether the Stratum V1 server is currently listening.
func (r *CoinRunner) StratumRunning() bool {
	if r.server == nil {
		return false
	}
	return r.server.IsRunning()
}

// SV2Running reports whether the Stratum V2 server is currently listening.
func (r *CoinRunner) SV2Running() bool {
	if r.sv2Server == nil {
		return false
	}
	return r.sv2Server.IsRunning()
}
