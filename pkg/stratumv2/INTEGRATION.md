# pkg/stratumv2 — Engine Integration Guide

This document explains exactly how to wire the `pkg/stratumv2` package into
`forgenx-engine` alongside the existing `pkg/stratum` (V1) server.

---

## 1. go.mod dependencies

Add to `go.mod`:

```
require (
    github.com/btcsuite/btcd/btcec/v2 v2.3.4
    github.com/flynn/noise           v1.1.0
)
```

Then:

```bash
go get github.com/btcsuite/btcd/btcec/v2@latest
go get github.com/flynn/noise@latest
go mod tidy
```

The `ellswift` sub-package ships inside `btcd/btcec/v2` — no separate import needed.

---

## 2. Static keypair persistence

The server's secp256k1 identity keypair must survive restarts so miners can
pin the server's public key. Store the 32-byte private key in the coin's
config file (e.g., `bch.json`) or in a dedicated file (e.g.,
`/pool/coins/bch_sv2.key`).

### In `pkg/config/config.go` — add to CoinConfig:

```go
// SV2StaticKeyPath is the path to the 32-byte secp256k1 private key file
// used for the SV2 Noise handshake. Auto-generated if absent.
SV2StaticKeyPath string `json:"sv2_static_key_path,omitempty"`

// SV2Port is the TCP port for the SV2 stratum server (default: V1port+1).
SV2Port int `json:"sv2_port,omitempty"`
```

### In `pkg/engine/coinrunner.go` — key loading at startup:

```go
import (
    "os"
    "github.com/ForgeNX/forgenx-engine/pkg/stratumv2"
)

func loadOrGenerateSV2Key(path string) (*stratumv2.StaticKeypair, error) {
    if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
        return stratumv2.LoadStaticKeypair(data)
    }
    // Generate a new keypair and persist it.
    kp, err := stratumv2.GenerateStaticKeypair()
    if err != nil {
        return nil, err
    }
    if err := os.WriteFile(path, kp.PrivKeyBytes(), 0600); err != nil {
        return nil, fmt.Errorf("save sv2 key: %w", err)
    }
    log.Printf("[sv2] generated new static keypair, saved to %s", path)
    return kp, nil
}
```

---

## 3. Starting the SV2 server

In `coinrunner.go` (or wherever V1 stratum is started), alongside the V1
`stratum.NewServer(...)` call:

```go
sv2KP, err := loadOrGenerateSV2Key(coin.SV2StaticKeyPath)
if err != nil {
    return fmt.Errorf("sv2 keypair: %w", err)
}

sv2Srv, err := stratumv2.NewServer(stratumv2.Config{
    ListenAddr:    fmt.Sprintf(":%d", coin.SV2Port),
    StaticKeypair: sv2KP,
    CoinTicker:    coin.Ticker,
    OnShare:       func(job *stratumv2.JobTemplate, ch *stratumv2.Channel, share *stratumv2.MsgSubmitSharesStandardFields, result *stratumv2.ShareResult) {
        if result.MeetsBlock {
            // Build and submit the solved block to the node RPC.
            go submitSV2Block(job, ch, share, result)
        }
        // Update worker stats in the metrics API.
        engine.UpdateWorkerStats(ch.UserIdentity(), result.Difficulty)
    },
})
if err != nil {
    return err
}

go func() {
    if err := sv2Srv.Start(); err != nil {
        log.Printf("[sv2-%s] server error: %v", coin.Ticker, err)
    }
}()
```

---

## 4. Block template broadcast (ZMQ integration)

The existing ZMQ subscriber in `pkg/noderpc/zmq.go` fires on `hashblock`
and `hashtx` topics. After the engine calls `getblocktemplate` on the RPC
client and builds a V1 job, it should also call `sv2Srv.BroadcastTemplate()`.

### In `pkg/engine/jobmanager.go` — after building V1 job:

```go
// Existing V1 dispatch:
v1Server.BroadcastJob(v1Job)

// New SV2 dispatch:
sv2Tmpl, err := buildSV2Template(gbtResult, nextJobID)
if err != nil {
    log.Printf("sv2 template build error: %v", err)
} else {
    sv2Srv.BroadcastTemplate(sv2Tmpl)
}
```

### `buildSV2Template` helper:

```go
func buildSV2Template(gbt *noderpc.BlockTemplate, jobID uint32) (*stratumv2.JobTemplate, error) {
    prevHash, err := stratumv2.PrevHashFromHex(gbt.PreviousBlockHash)
    if err != nil {
        return nil, err
    }

    branch, err := stratumv2.MerkleBranchFromHexSlice(gbt.MerkleBranch)
    if err != nil {
        return nil, err
    }

    // coinbase1/coinbase2 come from the coin's existing coinbase builder.
    // The split point is after the scriptSig height bytes, before extranonce.
    cb1, cb2 := coin.BuildCoinbaseSplit(gbt, poolAddress, extraData)

    return &stratumv2.JobTemplate{
        PrevHash:     prevHash,
        Coinbase1:    cb1,
        Coinbase2:    cb2,
        MerkleBranch: branch,
        Version:      uint32(gbt.Version),
        NBits:        parseNBits(gbt.Bits),   // hex string → uint32
        NTime:        uint32(time.Now().Unix()),
        Height:       uint32(gbt.Height),
        IsFutureJob:  false,
    }, nil
}
```

---

## 5. Block submission (`submitSV2Block`)

When `result.MeetsBlock == true`, construct and submit the block:

```go
func submitSV2Block(
    job *stratumv2.JobTemplate,
    ch *stratumv2.Channel,
    share *stratumv2.MsgSubmitSharesStandardFields,
    result *stratumv2.ShareResult,
) {
    // Reconstruct the miner's extranonce2 (4-byte LE sequence number).
    var en2 [4]byte
    binary.LittleEndian.PutUint32(en2[:], share.SequenceNum)

    // Build the full coinbase transaction bytes.
    coinbaseTxBytes := assembleCoinbaseTx(job.Coinbase1, ch.Extranonce1Bytes(), en2[:], job.Coinbase2)

    // Compute the coinbase hash and derive the Merkle root.
    coinbaseHash := stratumv2.CoinbaseHashForTemplate(job.Coinbase1, ch.Extranonce1Bytes(), en2[:], job.Coinbase2)
    merkleRoot := stratumv2.ComputeMerkleRoot(coinbaseHash, job.MerkleBranch)

    // Assemble the solved 80-byte header.
    hdr := stratumv2.BuildHeader(share.Version, job.PrevHash, merkleRoot, share.NTime, job.NBits, share.Nonce)

    // Serialize the full block (header + txcount + coinbase + other txs).
    blockHex := stratumv2.AssembleBlockHex(hdr, coinbaseTxBytes, gbtTransactions)

    // Submit to node RPC.
    if err := rpcClient.SubmitBlock(blockHex); err != nil {
        log.Printf("[sv2] submitblock failed: %v", err)
    } else {
        log.Printf("[sv2] *** BLOCK FOUND *** height=%d hash=%s", job.Height, result.HashHex)
    }
}
```

---

## 6. Metrics API exposure

The V1 metrics API (`pkg/metrics/api.go`) exposes pool stats at `/stats`.
Add SV2 stats to the existing response struct:

```go
type StatsResponse struct {
    // ... existing V1 fields ...
    SV2 *SV2Stats `json:"sv2,omitempty"`
}

type SV2Stats struct {
    ActiveSessions   int64  `json:"active_sessions"`
    TotalConnections uint64 `json:"total_connections"`
    TotalChannels    int    `json:"total_channels"`
    PublicKey        string `json:"public_key_ellswift"`
}
```

Populate from `sv2Srv.Stats()`.

---

## 7. Port allocation

| Protocol | Port |
|----------|------|
| Stratum V1 | 3334 |
| Stratum V2 | 3335 |

Expose 3335 in the engine's `docker-compose.yml`:

```yaml
ports:
  - "3334:3334"   # V1 stratum
  - "3335:3335"   # V2 stratum
```

And in `forgebch`'s `docker-compose.yml` (the coin app layer), add the
engine service port mapping for 3335.

---

## 8. File structure summary

```
pkg/
  stratum/          ← existing V1 (unchanged)
    server.go
    session.go
  stratumv2/        ← new SV2 package
    noise.go        ← Noise_NX handshake (secp256k1 + EllSwift)
    codec.go        ← SV2 binary frame encoder/decoder
    messages.go     ← all message type constants + encode/decode
    pow.go          ← target arithmetic, header hashing, share validation
    channel.go      ← Standard Mining Channel state
    session.go      ← per-connection state machine
    server.go       ← TCP listener + engine integration helpers
    stratumv2_test.go
    INTEGRATION.md  ← this file
```

---

## 9. Testing the handshake

Once running, test connectivity with the `stratum-ping` tool from
[demand-cli](https://github.com/demand-cli/stratum-ping) or any SV2-capable
miner in test mode:

```bash
stratum-ping sv2+tcp://192.168.1.105:3335
```

The server's EllSwift public key is logged at startup — miners can pin it
for verification.
