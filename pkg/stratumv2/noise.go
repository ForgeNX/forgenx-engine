// Package stratumv2 implements the Stratum V2 Mining Protocol for ForgeNX Engine.
// It runs as a standalone TCP server alongside the V1 stratum server, sharing
// the same JobManager and ZMQ block-template subscription.
//
// Protocol reference: https://stratumprotocol.org/specification/
//
// This file (noise.go) defines StaticKeypair — the server's per-coin secp256k1
// Noise-DH identity. The actual handshake protocol logic lives in sv2noise.go,
// which implements the REAL SV2-specific Noise_NX variant (secp256k1 + EllSwift
// DH directly, not a generic Curve25519 Noise Protocol Framework pattern).
//
// See sv2noise.go's header comment for why this split exists and what was
// wrong with the earlier flynn/noise-based implementation.
package stratumv2

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ellswift"
)

const (
	// ellSwiftLen is the wire size of an EllSwift-encoded secp256k1 public key.
	ellSwiftLen = 64

	// handshakeTimeoutSeconds is how long we wait for a miner to complete the
	// Noise handshake before dropping the connection.
	handshakeTimeoutSeconds = 10
)

// StaticKeypair holds the server's secp256k1 identity keypair used directly
// as the Noise DH static key (per the real SV2 spec — secp256k1 IS the DH
// curve, not a side-channel identity payload as in the earlier broken
// implementation). The EllSwift encoding cached here is regenerated fresh
// per call to ellswiftKeyMaterial() since BIP-324 EllSwift encoding is
// intentionally non-deterministic (see LoadStaticKeypair's doc comment).
type StaticKeypair struct {
	priv *btcec.PrivateKey
	pub  *btcec.PublicKey

	// ellSwiftPub is captured at construction time purely for logging
	// (EllSwiftPubKeyHex). It is NOT reused in the handshake itself —
	// ellswiftKeyMaterial() re-derives a fresh encoding for every connection,
	// since reusing a fixed EllSwift encoding across sessions would leak a
	// stable fingerprint the spec's randomized encoding is designed to avoid.
	ellSwiftPub [ellSwiftLen]byte
}

// GenerateStaticKeypair creates a new random secp256k1 keypair for the server.
func GenerateStaticKeypair() (*StaticKeypair, error) {
	priv, ellSwiftBytes, err := ellswift.EllswiftCreate()
	if err != nil {
		return nil, fmt.Errorf("stratumv2: generate static keypair: %w", err)
	}
	pub := priv.PubKey()
	return &StaticKeypair{
		priv:        priv,
		pub:         pub,
		ellSwiftPub: ellSwiftBytes,
	}, nil
}

// LoadStaticKeypair reconstructs a StaticKeypair from a 32-byte private key scalar.
// privKeyBytes must be a raw secp256k1 scalar (big-endian, 32 bytes).
//
// IMPORTANT: EllSwift encoding is intentionally non-deterministic by design
// (BIP-324) — re-encoding the SAME private key's public key produces a
// DIFFERENT 64-byte EllSwift representation on every call. This is correct
// protocol behavior: it prevents wire-level fingerprinting of the server's
// identity. The PRIVATE key is what persists and matters cryptographically;
// miners verify server identity through the handshake's certificate
// signature (see AuthorityKeypair / SignatureNoiseMessage in sv2noise.go),
// not by comparing EllSwift bytes across sessions.
func LoadStaticKeypair(privKeyBytes []byte) (*StaticKeypair, error) {
	if len(privKeyBytes) != 32 {
		return nil, errors.New("stratumv2: private key must be 32 bytes")
	}
	priv, pub := btcec.PrivKeyFromBytes(privKeyBytes)

	esBytes, err := ellswiftEncodePubkey(pub)
	if err != nil {
		return nil, err
	}

	return &StaticKeypair{
		priv:        priv,
		pub:         pub,
		ellSwiftPub: esBytes,
	}, nil
}

// ellswiftEncodePubkey derives a fresh (randomized) EllSwift encoding of an
// existing public key's X-coordinate via the real XElligatorSwift encoder
// (the same one EllswiftCreate uses internally for fresh keys).
func ellswiftEncodePubkey(pub *btcec.PublicKey) ([ellSwiftLen]byte, error) {
	var out [ellSwiftLen]byte

	compressed := pub.SerializeCompressed() // 33 bytes: 0x02/0x03 prefix + 32-byte X
	var xBytes [32]byte
	copy(xBytes[:], compressed[1:])

	var x btcec.FieldVal
	overflow := x.SetBytes(&xBytes)
	if overflow == 1 {
		x.Normalize()
	}

	u, t, err := ellswift.XElligatorSwift(&x)
	if err != nil {
		return out, fmt.Errorf("stratumv2: ellswift encode: %w", err)
	}

	uBytes := u.Bytes()
	tBytes := t.Bytes()
	copy(out[0:32], (*uBytes)[:])
	copy(out[32:64], (*tBytes)[:])
	return out, nil
}

// PrivKeyBytes returns the raw 32-byte private key scalar for persistence.
func (k *StaticKeypair) PrivKeyBytes() []byte {
	return k.priv.Serialize()
}

// EllSwiftPubKey returns the EllSwift encoding captured at construction time.
// For logging only — see the non-determinism note on LoadStaticKeypair.
func (k *StaticKeypair) EllSwiftPubKey() [ellSwiftLen]byte {
	return k.ellSwiftPub
}

// EllSwiftPubKeyHex returns EllSwiftPubKey() as a hex string for logging.
func (k *StaticKeypair) EllSwiftPubKeyHex() string {
	b := k.ellSwiftPub
	return hex.EncodeToString(b[:])
}

// xOnlyPubKeyBytes returns the 32-byte BIP-340 X-only encoding of this
// keypair's public key — used as the value the authority signs over in the
// handshake certificate (SignatureNoiseMessage).
func (k *StaticKeypair) xOnlyPubKeyBytes() [32]byte {
	var out [32]byte
	compressed := k.pub.SerializeCompressed()
	copy(out[:], compressed[1:])
	return out
}

// ellswiftKeyMaterial returns a FRESH EllSwift encoding for use in a single
// handshake (called once per connection by PerformSV2ServerHandshake). This
// is distinct from EllSwiftPubKey() — that one is a snapshot for logging;
// this one is freshly randomized per the spec's intent.
func (k *StaticKeypair) ellswiftKeyMaterial() (*btcec.PrivateKey, [ellSwiftLen]byte, error) {
	enc, err := ellswiftEncodePubkey(k.pub)
	if err != nil {
		return nil, enc, err
	}
	return k.priv, enc, nil
}
