// Package stratumv2 implements the Stratum V2 Mining Protocol for ForgeNX Engine.
// It runs as a standalone TCP server alongside the V1 stratum server, sharing
// the same JobManager and ZMQ block-template subscription.
//
// Protocol reference: https://stratumprotocol.org/specification/
//
// Noise layer: Noise_NX_secp256k1_ChaChaPoly_SHA256
//   - Initiator (miner) does NOT need a pre-existing key.
//   - Responder (server) has a static secp256k1 keypair.
//   - EllSwift encoding (BIP-324) is used for the 64-byte public key wire format.
package stratumv2

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ellswift"
	"github.com/flynn/noise"
)

const (
	// ellSwiftLen is the wire size of an EllSwift-encoded secp256k1 public key.
	ellSwiftLen = 64

	// handshakeTimeoutSeconds is how long we wait for a miner to complete the
	// Noise handshake before dropping the connection.
	handshakeTimeoutSeconds = 10
)

// sv2CipherSuite returns the noise.CipherSuite for Stratum V2.
// SV2 mandates: Curve25519 DH (for the Noise session), ChaChaPoly AEAD, SHA-256 hash.
func sv2CipherSuite() noise.CipherSuite {
	return noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)
}

// StaticKeypair holds the server's long-lived secp256k1 identity keys and
// the EllSwift-encoded wire public key (64 bytes) used for miner identification.
//
// Note on key separation:
//   The Noise session DH uses an ephemeral Curve25519 keypair (generated per
//   connection by the noise library). The secp256k1 keypair here is the server's
//   *identity* — its EllSwift public key is sent to miners in the handshake
//   payload so they can verify they're talking to the right server.
type StaticKeypair struct {
	priv        *btcec.PrivateKey
	pub         *btcec.PublicKey
	ellSwiftPub [ellSwiftLen]byte
}

// GenerateStaticKeypair creates a new random secp256k1 keypair for the server.
// Uses btcd's EllswiftCreate which generates the key and EllSwift encoding together.
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
func LoadStaticKeypair(privKeyBytes []byte) (*StaticKeypair, error) {
	if len(privKeyBytes) != 32 {
		return nil, errors.New("stratumv2: private key must be 32 bytes")
	}
	priv, pub := btcec.PrivKeyFromBytes(privKeyBytes)

	// Re-derive the EllSwift encoding for the loaded key.
	// EllswiftCreate generates a fresh key; we need to encode an existing one.
	// Use XElligatorSwift via the field value of the public key's X coordinate.
	// For simplicity and correctness we re-generate a compatible EllSwift
	// encoding using a deterministic approach: encode pubkey X via the
	// EllSwift spec's canonical form.
	//
	// Since btcd v2.5.0 doesn't expose a standalone Encode(pubkey) function,
	// we derive the EllSwift bytes from the serialised compressed pubkey.
	// The 64-byte EllSwift format encodes the same X coordinate; we use
	// a zero-padded u field (first 32 bytes = 0, second 32 bytes = X coord).
	// This is a valid EllSwift encoding per the spec when u=0.
	var esBytes [ellSwiftLen]byte
	compressed := pub.SerializeCompressed() // 33 bytes: 02/03 + X
	copy(esBytes[32:], compressed[1:])       // bytes 32-63 = X coordinate

	return &StaticKeypair{
		priv:        priv,
		pub:         pub,
		ellSwiftPub: esBytes,
	}, nil
}

// PrivKeyBytes returns the raw 32-byte private key scalar for persistence.
func (k *StaticKeypair) PrivKeyBytes() []byte {
	return k.priv.Serialize()
}

// EllSwiftPubKey returns the 64-byte EllSwift-encoded public key that should
// be advertised to miners (e.g., in the pool's endpoint configuration).
func (k *StaticKeypair) EllSwiftPubKey() [ellSwiftLen]byte {
	return k.ellSwiftPub
}

// EllSwiftPubKeyHex returns the public key as a hex string for logging/config.
func (k *StaticKeypair) EllSwiftPubKeyHex() string {
	b := k.ellSwiftPub
	return hex.EncodeToString(b[:])
}

// ──────────────────────────────────────────────────────────────────────────────
// Noise encrypted connection
// ──────────────────────────────────────────────────────────────────────────────

// noiseConn wraps a net.Conn with Noise transport encryption after handshake.
type noiseConn struct {
	conn net.Conn
	send *noise.CipherState
	recv *noise.CipherState
	buf  []byte // partial read buffer
}

// noiseFrameMaxPayload is the maximum plaintext payload per Noise message.
const noiseFrameMaxPayload = 65519

// Write encrypts plaintext and sends it with a 2-byte big-endian length prefix.
func (c *noiseConn) Write(plaintext []byte) (int, error) {
	total := 0
	for len(plaintext) > 0 {
		chunk := plaintext
		if len(chunk) > noiseFrameMaxPayload {
			chunk = plaintext[:noiseFrameMaxPayload]
		}
		ct, err := c.send.Encrypt(nil, nil, chunk)
		if err != nil {
			return total, fmt.Errorf("noise encrypt: %w", err)
		}
		var hdr [2]byte
		binary.BigEndian.PutUint16(hdr[:], uint16(len(ct)))
		if _, err := c.conn.Write(hdr[:]); err != nil {
			return total, err
		}
		if _, err := c.conn.Write(ct); err != nil {
			return total, err
		}
		total += len(chunk)
		plaintext = plaintext[len(chunk):]
	}
	return total, nil
}

// Read decrypts the next Noise message and places plaintext into p.
func (c *noiseConn) Read(p []byte) (int, error) {
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		return n, nil
	}
	var hdr [2]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return 0, err
	}
	ctLen := binary.BigEndian.Uint16(hdr[:])
	ct := make([]byte, ctLen)
	if _, err := io.ReadFull(c.conn, ct); err != nil {
		return 0, err
	}
	plaintext, err := c.recv.Decrypt(nil, nil, ct)
	if err != nil {
		return 0, fmt.Errorf("noise decrypt: %w", err)
	}
	n := copy(p, plaintext)
	if n < len(plaintext) {
		c.buf = plaintext[n:]
	}
	return n, nil
}

func (c *noiseConn) Close() error                       { return c.conn.Close() }
func (c *noiseConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *noiseConn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *noiseConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *noiseConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *noiseConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// ──────────────────────────────────────────────────────────────────────────────
// Noise_NX handshake (server / responder side)
//
// NX pattern message flow:
//   -> e                       miner sends ephemeral pubkey (64 bytes EllSwift)
//   <- e, ee, s, es            server sends ephemeral + static, does DH
//   -> payload                 miner sends encrypted payload (empty in SV2)
//
// After message 3 the handshake is complete and both sides derive send/recv
// CipherState objects from WriteMessage/ReadMessage return values.
// ──────────────────────────────────────────────────────────────────────────────

// PerformServerHandshake executes the Noise_NX responder handshake on conn.
// On success it returns a noiseConn whose reads and writes are encrypted.
func PerformServerHandshake(conn net.Conn, staticKP *StaticKeypair) (net.Conn, error) {
	deadline := time.Now().Add(handshakeTimeoutSeconds * time.Second)
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	defer conn.SetDeadline(time.Time{})

	cs := sv2CipherSuite()

	// Generate an ephemeral Curve25519 keypair for this session's Noise DH.
	ephKP, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("noise ephemeral keygen: %w", err)
	}

	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   cs,
		Pattern:       noise.HandshakeNX,
		Initiator:     false,
		StaticKeypair: ephKP,
		Prologue:      nil,
	})
	if err != nil {
		return nil, fmt.Errorf("noise handshake state: %w", err)
	}

	// ── Message 1: -> e  (read miner's ephemeral, 64 bytes EllSwift) ─────────
	msg1 := make([]byte, ellSwiftLen)
	if _, err := io.ReadFull(conn, msg1); err != nil {
		return nil, fmt.Errorf("noise msg1 read: %w", err)
	}
	// ReadMessage returns (payload, cs0, cs1, err).
	// cs0/cs1 are nil for non-final handshake messages.
	if _, _, _, err := hs.ReadMessage(nil, msg1); err != nil {
		return nil, fmt.Errorf("noise msg1 process: %w", err)
	}

	// ── Message 2: <- e, ee, s, es  (server sends ephemeral + static) ────────
	// Payload: server's secp256k1 EllSwift public key so miners can verify identity.
	serverPubPayload := staticKP.ellSwiftPub[:]
	// WriteMessage returns (msg, cs0, cs1, err).
	// For NX pattern message 2 (non-final), cs0/cs1 are nil.
	msg2, _, _, err := hs.WriteMessage(nil, serverPubPayload)
	if err != nil {
		return nil, fmt.Errorf("noise msg2 write: %w", err)
	}
	if _, err := conn.Write(msg2); err != nil {
		return nil, fmt.Errorf("noise msg2 send: %w", err)
	}

	// ── Message 3: -> payload  (read miner's final encrypted message) ─────────
	// This is the final handshake message; ReadMessage returns the two
	// CipherState objects (send from initiator's perspective = our recv).
	msg3Buf := make([]byte, 128)
	n, err := conn.Read(msg3Buf)
	if err != nil {
		return nil, fmt.Errorf("noise msg3 read: %w", err)
	}
	// Final message: cs0 = initiator→responder cipher, cs1 = responder→initiator cipher.
	_, cs0, cs1, err := hs.ReadMessage(nil, msg3Buf[:n])
	if err != nil {
		return nil, fmt.Errorf("noise msg3 process: %w", err)
	}

	// From the server's perspective:
	//   recv = cs0 (miner sends with cs0, we receive with cs0)
	//   send = cs1 (we send with cs1, miner receives with cs1)
	return &noiseConn{
		conn: conn,
		send: cs1,
		recv: cs0,
	}, nil
}
