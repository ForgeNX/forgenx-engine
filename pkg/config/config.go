/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package config

import (
	"encoding/json"
	"fmt"
	"os"

	coinpkg "github.com/ForgeNX/forgenx-engine/pkg/coin"
)

// Config is the top-level configuration for ForgeNX.
type Config struct {
	PoolName string                `json:"pool_name"`
	LogLevel string                `json:"log_level"`
	APIPort  int                   `json:"api_port"`
	DBPath   string                `json:"db_path"`
	Donation DonationConfig        `json:"donation"`
	Coins    map[string]CoinConfig `json:"coins"`
}

// DonationConfig holds developer donation settings.
// A small percentage of block rewards is donated to the project authors.
type DonationConfig struct {
	Enabled bool    `json:"enabled"`
	Percent float64 `json:"percent"` // percentage of block reward (default 1.0)
}

// CoinConfig holds per-coin configuration.
type CoinConfig struct {
	Enabled                 bool                    `json:"enabled"`
	CoinType                string                  `json:"coin_type"`
	CoinDefinition          *coinpkg.CoinDefinition `json:"coin_definition,omitempty"`
	Node                    NodeConfig              `json:"node"`
	Stratum                 StratumConfig           `json:"stratum"`
	Mining                  MiningConfig            `json:"mining"`
	VarDiff                 VarDiffConfig           `json:"vardiff"`
	TemplateRefreshInterval int                     `json:"template_refresh_interval"`
	IbdComplete             bool                    `json:"ibd_complete"`
	NodeOnline              bool                    `json:"node_online"`
	OwnerApp                string                  `json:"owner_app,omitempty"`
}

// NodeConfig holds blockchain node connection settings.
type NodeConfig struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	ZMQEnabled   bool   `json:"zmq_enabled"`
	ZMQHashBlock string `json:"zmq_hashblock"`
}

// StratumConfig holds stratum server settings.
type StratumConfig struct {
	Host              string  `json:"host"`
	Port              int     `json:"port"`
	Difficulty        float64 `json:"difficulty"`
	PingEnabled       bool    `json:"ping_enabled"`
	PingInterval      int     `json:"ping_interval"`                // seconds between server-sent pings (0 = disabled)
	AcceptSuggestDiff bool    `json:"accept_suggest_diff"`          // honor mining.suggest_difficulty from miners
	StaleShareGrace   int     `json:"stale_share_grace"`            // seconds to accept shares after a new block (default 5)
	LowDiffShareGrace int     `json:"low_diff_share_grace"`         // seconds to accept shares at previous diff after a change (default 5)
	SV2Enabled        bool    `json:"sv2_enabled"`                  // enable the Stratum V2 listener for this coin (default false)
	SV2Port           int     `json:"sv2_port,omitempty"`           // TCP port for the SV2 listener. Must be unique across V1 and all V2 listeners — NOT derived from Port.
	ConnectionTimeout int     `json:"connection_timeout,omitempty"` // seconds before dropping an idle SV2 miner (0 = default 600). Equivalent to GSS Connection Timeout.
}

// MiningConfig holds mining-related settings.
type MiningConfig struct {
	Mode           string `json:"mode"` // "pool" (default) or "solo"
	Address        string `json:"address"`
	Network        string `json:"network"`
	CoinbaseText   string `json:"coinbase_text"`
	ExtraNonceSize int    `json:"extranonce_size"`
}

// VarDiffConfig holds variable difficulty settings.
type VarDiffConfig struct {
	Enabled           bool    `json:"enabled"`
	MinDiff           float64 `json:"min_diff"`
	MaxDiff           float64 `json:"max_diff"`
	TargetTime        float64 `json:"target_time"`
	RetargetTime      float64 `json:"retarget_time"`
	VariancePct       float64 `json:"variance_percent"`
	FloatDiff         bool    `json:"float_diff"`
	FloatDiffBelowOne *bool   `json:"float_diff_below_one,omitempty"` // only use float for sub-1 difficulty, integer for >= 1 (default true)
	FloatPrecision    int     `json:"float_precision"`
	OnNewBlock        *bool   `json:"on_new_block,omitempty"` // only apply vardiff on clean jobs (default true)
}

// Load reads a JSON config file and returns a validated Config.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := &Config{}
			applyDefaults(cfg) // 👈 THIS is the fix
			return cfg, nil
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.PoolName == "" {
		cfg.PoolName = "ForgeNX"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.APIPort == 0 {
		cfg.APIPort = 8080
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "/pool/db/forgenx.db"
	}
	if cfg.Donation.Percent == 0 {
		cfg.Donation.Enabled = true
		cfg.Donation.Percent = 1.0
	}

	for symbol, coin := range cfg.Coins {
		if coin.Node.Host == "" {
			coin.Node.Host = "127.0.0.1"
		}
		if coin.Stratum.Host == "" {
			coin.Stratum.Host = "0.0.0.0"
		}
		if coin.Stratum.Difficulty == 0 {
			coin.Stratum.Difficulty = 1024
		}
		if coin.Stratum.PingEnabled && coin.Stratum.PingInterval == 0 {
			coin.Stratum.PingInterval = 30
		}
		if coin.Stratum.StaleShareGrace == 0 {
			coin.Stratum.StaleShareGrace = 5
		}
		if coin.Stratum.LowDiffShareGrace == 0 {
			coin.Stratum.LowDiffShareGrace = 5
		}
		// SV2 ports live in an entirely separate namespace from V1
		// stratum ports (1000 above, e.g. V1 3334 -> SV2 4334) so
		// adding coins never causes a V1/V2 port collision the way
		// a Port+1 offset would. Operators can still override via
		// sv2_port in the coin's config if a specific port is needed.
		if coin.Stratum.SV2Port == 0 {
			coin.Stratum.SV2Port = coin.Stratum.Port + 1000
		}
		if coin.Mining.CoinbaseText == "" {
			coin.Mining.CoinbaseText = "ForgeNX"
		}
		if coin.Mining.ExtraNonceSize == 0 {
			coin.Mining.ExtraNonceSize = 8
		}
		if coin.Mining.Mode == "" {
			coin.Mining.Mode = "pool"
		}
		if coin.Mining.Network == "" {
			coin.Mining.Network = "mainnet"
		}
		if coin.TemplateRefreshInterval == 0 {
			coin.TemplateRefreshInterval = 100
		}

		// VarDiff defaults
		if coin.VarDiff.MinDiff == 0 {
			coin.VarDiff.MinDiff = 512
		}
		if coin.VarDiff.MaxDiff == 0 {
			coin.VarDiff.MaxDiff = 32768
		}
		if coin.VarDiff.TargetTime == 0 {
			coin.VarDiff.TargetTime = 15
		}
		if coin.VarDiff.RetargetTime == 0 {
			coin.VarDiff.RetargetTime = 300
		}
		if coin.VarDiff.VariancePct == 0 {
			coin.VarDiff.VariancePct = 30
		}
		if coin.VarDiff.FloatPrecision == 0 {
			coin.VarDiff.FloatPrecision = 2
		}
		if coin.VarDiff.FloatDiffBelowOne == nil {
			t := true
			coin.VarDiff.FloatDiffBelowOne = &t
		}
		if coin.VarDiff.OnNewBlock == nil {
			t := true
			coin.VarDiff.OnNewBlock = &t
		}

		cfg.Coins[symbol] = coin
	}
}

func validate(cfg *Config) error {
	if len(cfg.Coins) == 0 {
		return fmt.Errorf("no coins configured")
	}

	// Collect every port (V1 stratum + SV2 stratum) across all enabled
	// coins to catch collisions before they cause a confusing runtime
	// "address already in use" error at startup.
	usedPorts := make(map[int]string) // port -> "symbol (v1|sv2)"
	checkPort := func(port int, label string) error {
		if port == 0 {
			return nil
		}
		if existing, taken := usedPorts[port]; taken {
			return fmt.Errorf("port %d used by both %s and %s", port, existing, label)
		}
		usedPorts[port] = label
		return nil
	}

	enabledCount := 0
	for symbol, coin := range cfg.Coins {
		if !coin.Enabled {
			continue
		}
		enabledCount++

		if err := checkPort(coin.Stratum.Port, fmt.Sprintf("%s (v1 stratum)", symbol)); err != nil {
			return err
		}
		if coin.Stratum.SV2Enabled {
			if err := checkPort(coin.Stratum.SV2Port, fmt.Sprintf("%s (sv2 stratum)", symbol)); err != nil {
				return err
			}
		}

		if coin.CoinType == "" {
			return fmt.Errorf("coin %s: coin_type is required", symbol)
		}

		// Validate coin_definition for non-built-in coin types
		if _, err := coinpkg.Get(coin.CoinType); err != nil {
			if coin.CoinDefinition == nil {
				return fmt.Errorf("coin %s: coin_type %q is not built-in; coin_definition is required", symbol, coin.CoinType)
			}
			if err := coinpkg.ValidateCoinDefinition(coin.CoinType, coin.CoinDefinition); err != nil {
				return fmt.Errorf("coin %s: %w", symbol, err)
			}
		}
		if coin.Node.Port == 0 {
			return fmt.Errorf("coin %s: node port is required", symbol)
		}
		if coin.Node.Username == "" || coin.Node.Password == "" {
			return fmt.Errorf("coin %s: node username and password are required", symbol)
		}
		if coin.Stratum.Port == 0 {
			return fmt.Errorf("coin %s: stratum port is required", symbol)
		}
		if coin.Mining.Mode != "pool" && coin.Mining.Mode != "solo" {
			return fmt.Errorf("coin %s: mining mode must be \"pool\" or \"solo\"", symbol)
		}
		if coin.Mining.Mode == "pool" && coin.Mining.Address == "" {
			return fmt.Errorf("coin %s: mining address is required in pool mode", symbol)
		}
		if coin.Mining.ExtraNonceSize < 2 || coin.Mining.ExtraNonceSize > 8 {
			return fmt.Errorf("coin %s: extranonce_size must be between 2 and 8", symbol)
		}
		if coin.Node.ZMQEnabled && coin.Node.ZMQHashBlock == "" {
			return fmt.Errorf("coin %s: zmq_hashblock is required when zmq_enabled is true", symbol)
		}
		if coin.VarDiff.Enabled {
			if coin.VarDiff.MinDiff <= 0 {
				return fmt.Errorf("coin %s: vardiff min_diff must be positive", symbol)
			}
			if coin.VarDiff.MaxDiff <= coin.VarDiff.MinDiff {
				return fmt.Errorf("coin %s: vardiff max_diff must be greater than min_diff", symbol)
			}
			if coin.VarDiff.TargetTime <= 0 {
				return fmt.Errorf("coin %s: vardiff target_time must be positive", symbol)
			}
		}
	}

	if enabledCount == 0 {
		// Allow zero coins — they may be loaded dynamically from /pool/coins
	}

	return nil
}
