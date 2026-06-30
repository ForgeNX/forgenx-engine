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
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/coin"
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
type CoinRunner struct {
	symbol          string
	coin            coin.Coin
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
}

// NewCoinRunner creates and wires up all components for a single coin.
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

	// Create job manager
	jobMgr := NewJobManager(JobManagerConfig{
		Coin:            c,
		RPCClient:       rpcClient,
		Address:         cfg.Mining.Address,
		Network:         cfg.Mining.Network,
		CoinbaseText:    cfg.Mining.CoinbaseText,
		ExtraNonceSize:  cfg.Mining.ExtraNonceSize,
		PollInterval:    time.Duration(cfg.TemplateRefreshInterval) * time.Second,
		SoloMode:        soloMode,
		DonationScript:  donationScript,
		DonationPercent: donationPercent,
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

		serverCfg.OnSessionRemoved = func(session *stratum.Session) {
			if addr := session.MiningAddress(); addr != "" {
				jobMgr.UnregisterAddress(addr)
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
				ListenAddr:       fmt.Sprintf("%s:%d", cfg.Stratum.Host, cfg.Stratum.SV2Port),
				StaticKeypair:    sv2KP,
				AuthorityKeypair: sv2AuthKP,
				CoinTicker:       symbol,
				OnShare: func(job *stratumv2.JobTemplate, ch *stratumv2.Channel, share *stratumv2.MsgSubmitSharesStandardFields, result *stratumv2.ShareResult) {
					if result.MeetsBlock {
						runner.logger.Info("[%s] *** SV2 BLOCK CANDIDATE FOUND *** worker=%q height=%d hash=%s",
							symbol, ch.UserIdentity(), job.Height, result.HashHex)
						// Block assembly/submission requires the full transaction set from
						// the originating template (template.Transactions). That set isn't
						// threaded through JobTemplate today — wire this in a follow-up
						// patch (submitSV2Block) before relying on SV2 to find real blocks.
						// Until then this is a CONNECTIVITY-ONLY deployment: shares are
						// validated and accepted correctly, but a found block will be
						// logged, not submitted.
					}
				},
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

			sv2Srv, err := stratumv2.NewServer(sv2Cfg)
			if err != nil {
				runner.logger.Warn("[%s] SV2 disabled: %v", symbol, err)
			} else {
				runner.sv2Server = sv2Srv
				authPubHex := hex.EncodeToString(func() []byte { b := sv2AuthKP.XOnlyPubKeyBytes(); return b[:] }())
				runner.logger.Info("[%s] SV2 server configured on %s (solo=%v) authority_pubkey=%s",
					symbol, sv2Cfg.ListenAddr, soloMode, authPubHex)
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
	return cr.server.Sessions()
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

func (r *CoinRunner) Hashrate() float64 {

	cutoff := time.Now().Add(-300 * time.Second)

	var diffSum float64
	var filtered []shareWork

	for _, s := range r.recentShares {
		if s.t.After(cutoff) {
			diffSum += s.diff
			filtered = append(filtered, s)
		}
	}

	// keep only recent shares
	r.recentShares = filtered

	if diffSum == 0 {
		return 0
	}

	hashes := diffSum * 4294967296

	return (hashes / 60) / 1e12
}
