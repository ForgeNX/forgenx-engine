package stratumv2

import (
	"encoding/hex"
	"fmt"
)

// ──────────────────────────────────────────────────────────────────────────────
// V1 → SV2 Job Adapter
//
// ForgeNX's JobManager (pkg/engine/jobmanager.go) builds jobs in the classic
// Stratum V1 wire format: hex strings, with PrevHash word-swapped for the V1
// protocol's specific byte ordering. SV2 needs raw internal byte order
// ([32]byte, true little-endian as used in block header hashing).
//
// This file converts V1's JobData fields into a stratumv2.JobTemplate without
// touching jobmanager.go at all — the engine's working V1 path is completely
// unmodified. The SV2 server is fed via this adapter inside the existing
// onNewJob callback in coinrunner.go.
//
// IMPORTANT byte-order notes (read before changing this file):
//
//   1. template.PreviousBlockHash (from getblocktemplate RPC) is in RPC
//      DISPLAY order (big-endian hex, the way block explorers show it).
//
//   2. jobmanager.go's swapHashEndianness() converts that display-order hash
//      into the STRATUM V1 wire format: it reverses all bytes (display →
//      internal LE), THEN swaps bytes within each 4-byte word. That second
//      step is V1-protocol-specific framing and must NOT be applied for SV2.
//
//   3. SV2's EncodeSetNewPrevHash / BuildHeader expect true internal byte
//      order: just a straight byte-reversal of the RPC display hex, with NO
//      word-swap. So for SV2 we re-derive prevHash from the RPC's raw
//      template.PreviousBlockHash via PrevHashFromHex(), NOT from job.PrevHash
//      (which is already word-swapped for V1 and would be wrong for SV2).
//
//   4. Coinb1/Coinb2 hex strings are byte-for-byte transaction data (not
//      affected by endianness concerns) — these decode directly to []byte.
//
//   5. MerkleBranches as built by computeStratumMerkleBranches() are already
//      in internal byte order (they're computed from reversed txid bytes via
//      coinbase.ReverseBytes before hashing) — same convention SV2 wants, so
//      these decode directly without re-reversing.
// ──────────────────────────────────────────────────────────────────────────────

// V1JobSource is the minimal set of fields the adapter needs from a V1
// JobData + its originating template. CoinRunner constructs this from
// JobData/BlockTemplate inside the onNewJob hook — see INTEGRATION.md.
type V1JobSource struct {
	JobIDHex          string   // JobData.Job.JobID, e.g. "0000000000000001"
	PrevBlockHashHex  string   // template.PreviousBlockHash — RPC DISPLAY order, NOT job.PrevHash
	Coinb1Hex         string   // JobData.Coinb1 (pool mode) or per-address coinb1 (solo mode)
	Coinb2Hex         string   // JobData.Coinb2 (pool mode) or per-address coinb2 (solo mode)
	MerkleBranchesHex []string // JobData.Job.MerkleBranches
	VersionHex        string   // JobData.Job.Version, 8 hex chars
	NBitsHex          string   // JobData.Job.NBits, 8 hex chars
	NTimeHex          string   // JobData.Job.NTime, 8 hex chars
	Height            uint32   // template.Height
	CleanJobs         bool     // JobData.Job.CleanJobs — true on new block tip

	// ExtraNonce2Size is the number of bytes the V1 coinbase reserved for a miner's
	// extranonce2. The scriptSig length in Coinb1Hex already accounts for it, so a
	// coinbase assembled without those bytes is short by exactly this many and the
	// node cannot decode it. Extended channels fill the space with the miner's own
	// extranonce2; standard channels have none and must pad it.
	ExtraNonce2Size int
}

// BuildTemplateFromV1Job converts a V1JobSource into a stratumv2.JobTemplate.
// JobID assignment is left to Server.BroadcastTemplate (it overwrites
// tmpl.JobID with its own atomic counter), so the value here is informational
// only and safe to leave as zero.
func BuildTemplateFromV1Job(src V1JobSource) (*JobTemplate, error) {
	prevHash, err := PrevHashFromHex(src.PrevBlockHashHex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: prevhash: %w", err)
	}

	coinbase1, err := hex.DecodeString(src.Coinb1Hex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: coinb1 decode: %w", err)
	}

	coinbase2, err := hex.DecodeString(src.Coinb2Hex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: coinb2 decode: %w", err)
	}

	branch, err := merkleBranchFromInternalOrderHex(src.MerkleBranchesHex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: merkle branch: %w", err)
	}

	version, err := hexToUint32(src.VersionHex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: version: %w", err)
	}

	nBits, err := hexToUint32(src.NBitsHex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: nbits: %w", err)
	}

	nTime, err := hexToUint32(src.NTimeHex)
	if err != nil {
		return nil, fmt.Errorf("sv2 adapter: ntime: %w", err)
	}

	return &JobTemplate{
		PrevHash:        prevHash,
		Coinbase1:       coinbase1,
		Coinbase2:       coinbase2,
		MerkleBranch:    branch,
		Version:         version,
		NBits:           nBits,
		NTime:           nTime,
		Height:          src.Height,
		IsFutureJob:     false,
		ExtraNonce2Size: src.ExtraNonce2Size,
	}, nil
}

// merkleBranchFromInternalOrderHex decodes a list of hex strings that are
// ALREADY in internal byte order (as produced by jobmanager.go's
// computeStratumMerkleBranches, which reverses txid bytes before hashing).
// Unlike MerkleBranchFromHexSlice (used for raw GBT data), this does NOT
// reverse the bytes again.
func merkleBranchFromInternalOrderHex(hexHashes []string) ([][32]byte, error) {
	branch := make([][32]byte, len(hexHashes))
	for i, h := range hexHashes {
		b, err := hex.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("branch[%d]: %w", i, err)
		}
		if len(b) != 32 {
			return nil, fmt.Errorf("branch[%d]: expected 32 bytes, got %d", i, len(b))
		}
		copy(branch[i][:], b)
	}
	return branch, nil
}

// hexToUint32 parses an 8-character (or shorter) hex string as a big-endian
// uint32. JobManager formats Version/NBits/NTime via fmt.Sprintf("%08x", ...),
// i.e. standard big-endian hex representation of the numeric value.
func hexToUint32(s string) (uint32, error) {
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}
	var v uint64
	_, err := fmt.Sscanf(s, "%x", &v)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}
