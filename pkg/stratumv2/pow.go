package stratumv2

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
)

// ──────────────────────────────────────────────────────────────────────────────
// SV2 PoW Utilities
//
// Handles:
//   • nBits compact target → 256-bit target conversion
//   • 256-bit target → difficulty conversion
//   • Block hash computation (double-SHA256 of 80-byte header)
//   • Share validation: does the hash meet the channel's current target?
// ──────────────────────────────────────────────────────────────────────────────

var (
	// bigOne is used repeatedly in difficulty calculations.
	bigOne = big.NewInt(1)

	// maxTarget is the easiest possible target (all bits set except the sign bit).
	// Equivalent to SHA-256d difficulty 1.
	maxTarget = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 256), bigOne)

	// diff1Target is the standard Bitcoin difficulty-1 target:
	// 0x00000000FFFF0000000000000000000000000000000000000000000000000000
	diff1TargetHex = "00000000ffff0000000000000000000000000000000000000000000000000000"
	diff1Target, _ = new(big.Int).SetString(diff1TargetHex, 16)
)

// NBitsToTarget converts a compact nBits value to a 256-bit big.Int target.
// The nBits format is the same used in Bitcoin block headers.
func NBitsToTarget(nBits uint32) *big.Int {
	// nBits format: [exp:8][coeff:24]
	// target = coeff * 2^(8*(exp-3))
	exp := int(nBits >> 24)
	coeff := nBits & 0x00FFFFFF

	t := new(big.Int).SetInt64(int64(coeff))
	if exp <= 3 {
		t.Rsh(t, uint(8*(3-exp)))
	} else {
		t.Lsh(t, uint(8*(exp-3)))
	}
	return t
}

// TargetToBytes converts a big.Int target to a 32-byte little-endian slice,
// as required by SV2's B0_32 target fields.
func TargetToBytes(t *big.Int) []byte {
	b := make([]byte, 32)
	raw := t.Bytes() // big-endian, variable length
	// Copy into the HIGH end of b (big-endian), then reverse to LE.
	copy(b[32-len(raw):], raw)
	reverseBytes(b)
	return b
}

// BytesToTarget converts a 32-byte little-endian SV2 target back to big.Int.
func BytesToTarget(b []byte) *big.Int {
	if len(b) != 32 {
		return big.NewInt(0)
	}
	// Make a copy, reverse to big-endian, then parse.
	be := make([]byte, 32)
	copy(be, b)
	reverseBytes(be)
	return new(big.Int).SetBytes(be)
}

// TargetToDifficulty converts a target to a human-readable difficulty float.
// difficulty = diff1Target / target
func TargetToDifficulty(target *big.Int) float64 {
	if target.Sign() == 0 {
		return 0
	}
	d := new(big.Float).SetInt(diff1Target)
	t := new(big.Float).SetInt(target)
	result, _ := new(big.Float).Quo(d, t).Float64()
	return result
}

// DifficultyToTarget converts a pool-side difficulty to a 256-bit target.
// target = diff1Target / difficulty
func DifficultyToTarget(difficulty float64) *big.Int {
	if difficulty <= 0 {
		return new(big.Int).Set(maxTarget)
	}
	// Use big.Float for precision.
	d := new(big.Float).SetFloat64(difficulty)
	d1 := new(big.Float).SetInt(diff1Target)
	tFloat := new(big.Float).Quo(d1, d)
	t, _ := tFloat.Int(nil)
	return t
}

// ──────────────────────────────────────────────────────────────────────────────
// Block Header Hashing
// ──────────────────────────────────────────────────────────────────────────────

// HashBlockHeader computes the double-SHA256 of a standard 80-byte block header.
// The result is returned in internal byte order (little-endian, as used in
// comparisons against the target).
func HashBlockHeader(header [80]byte) [32]byte {
	first := sha256.Sum256(header[:])
	return sha256.Sum256(first[:])
}

// BuildHeader assembles the 80-byte block header from its components.
// All multi-byte fields are little-endian as per the Bitcoin wire format.
//
//	version(4) + prevHash(32) + merkleRoot(32) + nTime(4) + nBits(4) + nonce(4)
func BuildHeader(
	version uint32,
	prevHash [32]byte,
	merkleRoot [32]byte,
	nTime uint32,
	nBits uint32,
	nonce uint32,
) [80]byte {
	var hdr [80]byte
	binary.LittleEndian.PutUint32(hdr[0:4], version)
	copy(hdr[4:36], prevHash[:])
	copy(hdr[36:68], merkleRoot[:])
	binary.LittleEndian.PutUint32(hdr[68:72], nTime)
	binary.LittleEndian.PutUint32(hdr[72:76], nBits)
	binary.LittleEndian.PutUint32(hdr[76:80], nonce)
	return hdr
}

// ──────────────────────────────────────────────────────────────────────────────
// Share Validation
// ──────────────────────────────────────────────────────────────────────────────

// ShareResult is the outcome of validating a submitted share.
type ShareResult struct {
	Hash       [32]byte // double-SHA256 of the block header
	HashHex    string   // display hex (reversed to "display" byte order)
	Difficulty float64  // share's actual difficulty
	MeetsPool  bool     // hash ≤ channel's current pool target
	MeetsBlock bool     // hash ≤ block's network target (nBits from template)
}

// ValidateShare checks a miner's submitted share against the channel's pool
// target and the current network target.
//
//	version:      from the share submission (may have rolled bits)
//	prevHash:     from the current job template
//	merkleRoot:   computed from coinbase + Merkle branch
//	nTime:        from the share submission (must be ≥ minNtime)
//	nBits:        from the current block template
//	nonce:        from the share submission
//	poolTarget:   the channel's current pool-difficulty target (B0_32 LE bytes)
//	networkNBits: compact nBits from the template, used for block submission check
func ValidateShare(
	version uint32,
	prevHash [32]byte,
	merkleRoot [32]byte,
	nTime uint32,
	nBits uint32,
	nonce uint32,
	poolTarget []byte,
	networkNBits uint32,
) (*ShareResult, error) {
	if len(poolTarget) != 32 {
		return nil, fmt.Errorf("sv2 ValidateShare: poolTarget must be 32 bytes, got %d", len(poolTarget))
	}

	hdr := BuildHeader(version, prevHash, merkleRoot, nTime, nBits, nonce)
	hash := HashBlockHeader(hdr)

	// Convert hash to big.Int for comparison (internal byte order, LE → BE).
	hashBE := make([]byte, 32)
	copy(hashBE, hash[:])
	reverseBytes(hashBE)
	hashInt := new(big.Int).SetBytes(hashBE)

	// Compare against pool target.
	poolTargetInt := BytesToTarget(poolTarget)
	meetsPool := hashInt.Cmp(poolTargetInt) <= 0

	// Compare against network target.
	networkTarget := NBitsToTarget(networkNBits)
	meetsBlock := hashInt.Cmp(networkTarget) <= 0

	// Display hex: reverse hash bytes (display convention).
	displayHash := make([]byte, 32)
	copy(displayHash, hash[:])
	reverseBytes(displayHash)

	diff := TargetToDifficulty(hashInt)

	return &ShareResult{
		Hash:       hash,
		HashHex:    hex.EncodeToString(displayHash),
		Difficulty: diff,
		MeetsPool:  meetsPool,
		MeetsBlock: meetsBlock,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Merkle Root Computation
// ──────────────────────────────────────────────────────────────────────────────

// ComputeMerkleRoot derives the Merkle root from the coinbase transaction hash
// and the list of Merkle branch hashes, as provided by the block template.
//
// SV2's NewMiningJob sends a merkle_path (the branch), not the full tree.
// The miner computes:
//
//	root = hash(hash(coinbase) || branch[0])
//	root = hash(root || branch[1])
//	... and so on.
func ComputeMerkleRoot(coinbaseTxHash [32]byte, branch [][32]byte) [32]byte {
	current := coinbaseTxHash
	for _, node := range branch {
		current = merkleHashPair(current, node)
	}
	return current
}

// merkleHashPair double-SHA256s the concatenation of two 32-byte hashes.
func merkleHashPair(a, b [32]byte) [32]byte {
	var combined [64]byte
	copy(combined[:32], a[:])
	copy(combined[32:], b[:])
	first := sha256.Sum256(combined[:])
	return sha256.Sum256(first[:])
}

// HashCoinbaseTx computes the double-SHA256 hash of the coinbase transaction
// bytes (coinbase1 + extranonce1 + extranonce2 + coinbase2).
func HashCoinbaseTx(coinbase1, extranonce1, extranonce2, coinbase2 []byte) [32]byte {
	txBytes := make([]byte, 0, len(coinbase1)+len(extranonce1)+len(extranonce2)+len(coinbase2))
	txBytes = append(txBytes, coinbase1...)
	txBytes = append(txBytes, extranonce1...)
	txBytes = append(txBytes, extranonce2...)
	txBytes = append(txBytes, coinbase2...)
	first := sha256.Sum256(txBytes)
	return sha256.Sum256(first[:])
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func reverseBytes(b []byte) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}
