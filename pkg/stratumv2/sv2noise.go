// Package stratumv2 implements the Stratum V2 Mining Protocol for ForgeNX Engine.
//
// This file (sv2noise.go) implements the REAL SV2 Noise_NX handshake, ported
// directly from the Stratum Reference Implementation (SRI) Rust source:
//
//	https://github.com/stratum-mining/stratum/tree/main/sv2/noise-sv2/src
//
// CRITICAL: this is NOT a generic Noise Protocol Framework handshake. SV2
// defines its own bespoke variant — "Noise_NX_Secp256k1+EllSwift_ChaChaPoly_SHA256"
// — that uses secp256k1 + ElligatorSwift encoding directly as the Diffie-Hellman
// primitive (not Curve25519, which is what generic Noise libraries like
// flynn/noise provide). An earlier version of this file wrapped flynn/noise
// with Curve25519 DH and was confirmed via live testing against real Bitaxe
// Gamma SV2 firmware to be cryptographically incompatible — the handshake
// transcripts diverge from the very first hash, since the protocol name
// strings differ ("Noise_NX_25519_ChaChaPoly_SHA256" vs the real SV2 name).
//
// Spec reference: https://github.com/stratum-mining/sv2-spec/blob/main/04-Protocol-Security.md
package stratumv2

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ellswift"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"golang.org/x/crypto/chacha20poly1305"
)

// ──────────────────────────────────────────────────────────────────────────────
// Spec-defined size constants
// Source: sv2/noise-sv2/src/lib.rs
// ──────────────────────────────────────────────────────────────────────────────

const (
	aeadMACLen = 16

	sigNoiseMsgSize         = 74                           // version(2) + valid_from(4) + not_valid_after(4) + signature(64)
	encSigNoiseMsgSize      = sigNoiseMsgSize + aeadMACLen // 90
	ellswiftEncodingSize    = 64
	encEllswiftEncodingSize = ellswiftEncodingSize + aeadMACLen // 80

	// initiatorExpectedHandshakeMsgSize is the total size of the SINGLE
	// response message the responder (server) sends back to the initiator
	// (miner) in step 1 of the handshake:
	//   ephemeral EllSwift (64) + encrypted static EllSwift (80) + encrypted signature (90) = 234
	initiatorExpectedHandshakeMsgSize = ellswiftEncodingSize + encEllswiftEncodingSize + encSigNoiseMsgSize

	sv2CertVersion = uint16(0)
)

// noiseHashedProtocolNameChaCha is SHA256("Noise_NX_Secp256k1+EllSwift_ChaChaPoly_SHA256"),
// precomputed exactly as in the Rust reference (sv2/noise-sv2/src/lib.rs).
// This MUST match byte-for-byte or the handshake transcript diverges from message 1.
var noiseHashedProtocolNameChaCha = [32]byte{
	46, 180, 120, 129, 32, 142, 158, 238, 31, 102, 159, 103, 198, 110, 231, 14, 169, 234, 136, 9,
	13, 80, 63, 232, 48, 220, 75, 200, 62, 41, 191, 16,
}

// ──────────────────────────────────────────────────────────────────────────────
// Authority Keypair
//
// SV2 servers sign their per-session static key with a separate, longer-lived
// "authority" keypair, producing a certificate-like SignatureNoiseMessage the
// client can optionally verify against a known authority pubkey (out of band,
// e.g. operator publishes it). This is the X.509-style trust root for SV2.
// ──────────────────────────────────────────────────────────────────────────────

// AuthorityKeypair is the pool operator's long-lived signing identity.
// Distinct from StaticKeypair (the per-session/per-coin Noise DH key) —
// the authority key SIGNS the static key's certificate; it is not used in
// the Noise DH exchange itself.
type AuthorityKeypair struct {
	priv *btcec.PrivateKey
	pub  *btcec.PublicKey
}

// GenerateAuthorityKeypair creates a new random authority signing keypair.
// Persist PrivKeyBytes() so the same authority identity survives restarts —
// unlike the per-handshake EllSwift bytes, this key SHOULD stay stable, since
// operators are expected to publish/pin the authority pubkey out of band.
func GenerateAuthorityKeypair() (*AuthorityKeypair, error) {
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("stratumv2: generate authority keypair: %w", err)
	}
	return &AuthorityKeypair{priv: priv, pub: priv.PubKey()}, nil
}

// LoadAuthorityKeypair reconstructs an AuthorityKeypair from a 32-byte private key.
func LoadAuthorityKeypair(privKeyBytes []byte) (*AuthorityKeypair, error) {
	if len(privKeyBytes) != 32 {
		return nil, errors.New("stratumv2: authority private key must be 32 bytes")
	}
	priv, pub := btcec.PrivKeyFromBytes(privKeyBytes)
	return &AuthorityKeypair{priv: priv, pub: pub}, nil
}

// PrivKeyBytes returns the raw 32-byte private key scalar for persistence.
func (k *AuthorityKeypair) PrivKeyBytes() []byte {
	return k.priv.Serialize()
}

// XOnlyPubKeyBytes returns the 32-byte BIP-340 X-only public key encoding —
// this is the value operators publish/pin as the "authority pubkey".
func (k *AuthorityKeypair) XOnlyPubKeyBytes() [32]byte {
	var out [32]byte
	compressed := k.pub.SerializeCompressed()
	copy(out[:], compressed[1:]) // drop the 0x02/0x03 parity prefix byte
	return out
}

// ──────────────────────────────────────────────────────────────────────────────
// SignatureNoiseMessage
//
// version(2 LE) + valid_from(4 LE) + not_valid_after(4 LE) + schnorr_sig(64) = 74 bytes
// Signed message: SHA256(version || valid_from || not_valid_after || static_xonly_pubkey)
// ──────────────────────────────────────────────────────────────────────────────

// buildSignatureNoiseMessage constructs and signs the 74-byte certificate
// message binding staticXOnlyPubkey to authKP for the given validity window.
func buildSignatureNoiseMessage(
	authKP *AuthorityKeypair,
	staticXOnlyPubkey [32]byte,
	validFrom, notValidAfter uint32,
) ([sigNoiseMsgSize]byte, error) {
	var msg [sigNoiseMsgSize]byte
	binary.LittleEndian.PutUint16(msg[0:2], sv2CertVersion)
	binary.LittleEndian.PutUint32(msg[2:6], validFrom)
	binary.LittleEndian.PutUint32(msg[6:10], notValidAfter)

	// m = SHA256(msg[0:10] || static_xonly_pubkey), per signature_message.rs sign_with_rng.
	toHash := make([]byte, 0, 10+32)
	toHash = append(toHash, msg[0:10]...)
	toHash = append(toHash, staticXOnlyPubkey[:]...)
	digest := sha256.Sum256(toHash)

	sig, err := schnorr.Sign(authKP.priv, digest[:])
	if err != nil {
		return msg, fmt.Errorf("stratumv2: schnorr sign certificate: %w", err)
	}
	copy(msg[10:74], sig.Serialize())
	return msg, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SV2 Noise state machine primitives
// Direct port of sv2/noise-sv2/src/handshake.rs's HandshakeOp trait.
// ──────────────────────────────────────────────────────────────────────────────

// sv2HandshakeState holds the running h (handshake hash) / ck (chaining key) /
// k (handshake encryption key, once derived) / n (nonce counter) used while
// processing the single-message SV2 NX handshake.
type sv2HandshakeState struct {
	h      [32]byte
	ck     [32]byte
	k      *[32]byte // nil until mixKey() is first called
	n      uint64
	cipher cipher.AEAD // lazily constructed by initializeKey; nil until first mixKey
}

// newSV2HandshakeState initializes h/ck per HandshakeOp::initialize_self.
func newSV2HandshakeState() *sv2HandshakeState {
	ck := noiseHashedProtocolNameChaCha
	h := sha256.Sum256(ck[:])
	return &sv2HandshakeState{h: h, ck: ck}
}

// mixHash: h = SHA256(h || data)
func (s *sv2HandshakeState) mixHash(data []byte) {
	toHash := make([]byte, 0, 32+len(data))
	toHash = append(toHash, s.h[:]...)
	toHash = append(toHash, data...)
	s.h = sha256.Sum256(toHash)
}

// hmacHash implements HandshakeOp::hmac_hash — standard HMAC-SHA256.
func hmacHash(key []byte, data []byte) [32]byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// hkdf2 implements HandshakeOp::hkdf_2 exactly (Noise-style 2-output HKDF).
func hkdf2(chainingKey [32]byte, inputKeyMaterial []byte) (out1, out2 [32]byte) {
	tempKey := hmacHash(chainingKey[:], inputKeyMaterial)
	out1 = hmacHash(tempKey[:], []byte{0x01})
	cat := append(append([]byte{}, out1[:]...), 0x02)
	out2 = hmacHash(tempKey[:], cat)
	return out1, out2
}

// mixKey implements HandshakeOp::mix_key: derives a new ck/k pair from the
// current ck and fresh input key material (an ECDH output), then
// (re)initializes the handshake AEAD cipher with the new k.
func (s *sv2HandshakeState) mixKey(inputKeyMaterial []byte) error {
	newCk, tempK := hkdf2(s.ck, inputKeyMaterial)
	s.ck = newCk
	return s.initializeKey(tempK)
}

// initializeKey implements HandshakeOp::initialize_key: resets the nonce and
// constructs a fresh ChaCha20-Poly1305 AEAD cipher from the given key.
func (s *sv2HandshakeState) initializeKey(key [32]byte) error {
	s.n = 0
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return fmt.Errorf("stratumv2: init chacha20poly1305: %w", err)
	}
	s.cipher = aead
	kCopy := key
	s.k = &kCopy
	return nil
}

// nonceBytes implements CipherState::nonce_to_bytes: the 64-bit counter is
// placed in the LAST 8 bytes of a 12-byte little-endian nonce (first 4 zero).
func (s *sv2HandshakeState) nonceBytes() [12]byte {
	var out [12]byte
	binary.LittleEndian.PutUint64(out[4:], s.n)
	return out
}

// encryptAndHash implements HandshakeOp::encrypt_and_hash: if a handshake
// cipher is established, AEAD-encrypts plaintext in place (AD = current h),
// then mixes the resulting ciphertext into h. If no cipher yet, this is a
// no-op pass-through (matches the Rust behavior for the very first mix).
func (s *sv2HandshakeState) encryptAndHash(plaintext []byte) ([]byte, error) {
	var ciphertext []byte
	if s.cipher != nil {
		nonce := s.nonceBytes()
		s.n++
		ciphertext = s.cipher.Seal(nil, nonce[:], plaintext, s.h[:])
	} else {
		ciphertext = plaintext
	}
	s.mixHash(ciphertext)
	return ciphertext, nil
}

// decryptAndHash implements HandshakeOp::decrypt_and_hash: mirror of
// encryptAndHash for the receive side. Used during handshake message 1
// processing (empty payload from the initiator, so this just advances h).
func (s *sv2HandshakeState) decryptAndHash(ciphertext []byte) ([]byte, error) {
	var plaintext []byte
	var err error
	if s.cipher != nil {
		nonce := s.nonceBytes()
		s.n++
		plaintext, err = s.cipher.Open(nil, nonce[:], ciphertext, s.h[:])
		if err != nil {
			return nil, fmt.Errorf("stratumv2: decrypt_and_hash AEAD open failed: %w", err)
		}
	} else {
		plaintext = ciphertext
	}
	s.mixHash(ciphertext)
	return plaintext, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// BIP-324 ElligatorSwift X-only ECDH ("ellswift_xdh")
//
// CRITICAL: btcd's ellswift.EllswiftECDHXOnly returns ONLY the raw X-coordinate
// of the ECDH point — it does NOT perform the additional hashing step that
// BIP-324 (and therefore SV2, which reuses this primitive) requires. The real
// algorithm, per bitcoin-core/secp256k1's secp256k1_ellswift_xdh with the
// "bip324" hash function (src/modules/ellswift/main_impl.h):
//
//   shared_secret = tagged_hash("bip324_ellswift_xonly_ecdh", ell_a || ell_b || x)
//
// where x is the raw X-coordinate of priv·theirPubkey, and ell_a / ell_b are
// the two parties' 64-byte EllSwift encodings in a FIXED order determined by
// which one is "theirs" vs "ours" — NOT simply theirs-then-ours or vice versa.
// Per rust-secp256k1's Party::A/B → ffi 0/1 mapping and the C source's
// `theirs64 = party ? ell_a64 : ell_b64`, SRI's responder.rs calls always pass
// (theirs, ours, ..., Party::B) — meaning ell_a=theirs, ell_b=ours for BOTH
// the ee and es derivations on the responder side. We replicate that exact
// argument order here.
//
// tagged_hash(tag, msg) = SHA256(SHA256(tag) || SHA256(tag) || msg) — the
// standard BIP-340 tagged hash construction. bitcoin-core's C implementation
// uses a precomputed SHA256 midstate as a performance optimization; computing
// it from the tag string directly (as done here) produces an identical result
// and is far easier to verify correct against the spec.
// ──────────────────────────────────────────────────────────────────────────────

var bip324EllswiftTag = sha256.Sum256([]byte("bip324_ellswift_xonly_ecdh"))

// bip324TaggedHash computes BIP-340 tagged_hash(bip324EllswiftTag, msg).
func bip324TaggedHash(msg []byte) [32]byte {
	h := sha256.New()
	h.Write(bip324EllswiftTag[:])
	h.Write(bip324EllswiftTag[:])
	h.Write(msg)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// bip324EllswiftXDH computes the real SV2/BIP-324 shared secret:
//
//	tagged_hash("bip324_ellswift_xonly_ecdh", ellA || ellB || x(priv·theirPub))
//
// ellTheirs/ellOurs are passed positionally as ellA=theirs, ellB=ours, matching
// SRI responder.rs's consistent (theirs, ours, ..., Party::B) call pattern.
func bip324EllswiftXDH(ellTheirs [ellswiftEncodingSize]byte, ellOurs [ellswiftEncodingSize]byte, ourPriv *btcec.PrivateKey) ([32]byte, error) {
	rawX, err := ellswift.EllswiftECDHXOnly(ellTheirs, ourPriv)
	if err != nil {
		return [32]byte{}, fmt.Errorf("stratumv2: raw ellswift ECDH: %w", err)
	}

	msg := make([]byte, 0, ellswiftEncodingSize*2+32)
	msg = append(msg, ellTheirs[:]...) // ell_a = theirs
	msg = append(msg, ellOurs[:]...)   // ell_b = ours
	msg = append(msg, rawX[:]...)

	return bip324TaggedHash(msg), nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Transport-phase ciphers (post-handshake)
//
// Direct port of responder.rs step_1's final cipher derivation:
//   (temp_k1, temp_k2) = hkdf_2(ck, [])
//   c1 = ChaCha20Poly1305(temp_k1)  — decrypts initiator→responder traffic
//   c2 = ChaCha20Poly1305(temp_k2)  — encrypts responder→initiator traffic
// ──────────────────────────────────────────────────────────────────────────────

// sv2TransportCipher wraps one direction of post-handshake AEAD traffic.
// Nonces increment per message exactly like the handshake-phase nonce.
type sv2TransportCipher struct {
	aead  cipher.AEAD
	nonce uint64
}

func newSV2TransportCipher(key [32]byte) (*sv2TransportCipher, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("stratumv2: init transport cipher: %w", err)
	}
	return &sv2TransportCipher{aead: aead}, nil
}

func (c *sv2TransportCipher) nonceBytes() [12]byte {
	var out [12]byte
	binary.LittleEndian.PutUint64(out[4:], c.nonce)
	return out
}

// seal encrypts plaintext with empty AAD (matches GenericCipher::encrypt,
// which always uses &[] as AD post-handshake).
func (c *sv2TransportCipher) seal(plaintext []byte) []byte {
	nonce := c.nonceBytes()
	c.nonce++
	return c.aead.Seal(nil, nonce[:], plaintext, nil)
}

// open decrypts ciphertext with empty AAD.
func (c *sv2TransportCipher) open(ciphertext []byte) ([]byte, error) {
	nonce := c.nonceBytes()
	c.nonce++
	pt, err := c.aead.Open(nil, nonce[:], ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("stratumv2: transport AEAD open failed: %w", err)
	}
	return pt, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// PerformSV2ServerHandshake — the real, spec-correct SV2 responder handshake.
//
// Single round trip:
//   <- e                         (initiator sends 64-byte EllSwift ephemeral pubkey)
//   -> e, ee, s, es, SIGNATURE    (responder sends 234-byte combined response)
// Transport phase begins immediately after — no third message, unlike a
// generic Noise NX pattern. This matches the Bitaxe firmware's observed
// behavior exactly (it sends once, then waits for one response).
// ──────────────────────────────────────────────────────────────────────────────

// PerformSV2ServerHandshake executes the real SV2 Noise_NX responder role.
// staticKP is this session's/coin's secp256k1 Noise-DH identity. authKP signs
// staticKP's certificate so clients with a pinned authority pubkey can verify
// server identity; clients without one (like the Bitaxe today) skip
// verification but still require a syntactically valid signed certificate to
// parse the response correctly.
//
// Returns the two transport-phase ciphers directly (NOT a wrapped net.Conn —
// see codec.go's header comment for why: SV2's encrypted wire framing has no
// length prefix, so the message codec must hold the ciphers itself to know
// how many bytes to read at each step). Pass these to NewCodec along with
// the original (still-plaintext-at-the-TCP-level, but now post-handshake)
// conn to build a working Codec for the transport phase.
func PerformSV2ServerHandshake(conn net.Conn, staticKP *StaticKeypair, authKP *AuthorityKeypair) (send, recv *sv2TransportCipher, err error) {
	deadline := time.Now().Add(handshakeTimeoutSeconds * time.Second)
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, nil, err
	}
	defer conn.SetDeadline(time.Time{})

	hs := newSV2HandshakeState()

	// ── Receive: <- e (64-byte EllSwift ephemeral pubkey from initiator) ─────
	var theirEphemeral [ellswiftEncodingSize]byte
	if _, err := io.ReadFull(conn, theirEphemeral[:]); err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: read initiator ephemeral: %w", err)
	}

	// Step 4.5.1.2 (Responder): MixHash(e.public_key), then DecryptAndHash([])
	// — the initiator sends no payload alongside its ephemeral key, but the
	// protocol still requires this hash-advancing call.
	hs.mixHash(theirEphemeral[:])
	if _, err := hs.decryptAndHash(nil); err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: decrypt_and_hash(empty): %w", err)
	}

	// ── Generate our own ephemeral keypair for this handshake ────────────────
	ourEphemeralPriv, ourEphemeralEllSwift, err := ellswift.EllswiftCreate()
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: generate ephemeral: %w", err)
	}

	// Step 4.5.2.1 (Responder): append e.public_key (ElligatorSwift, 64 bytes)
	out := make([]byte, 0, initiatorExpectedHandshakeMsgSize)
	out = append(out, ourEphemeralEllSwift[:]...)

	// MixHash(e.public_key) — our own ephemeral, in EllSwift-encoded form.
	hs.mixHash(ourEphemeralEllSwift[:])

	// MixKey(ECDH(e.private_key, re.public_key)) — ephemeral-ephemeral DH.
	// Uses the real BIP-324 tagged-hash shared secret, NOT a raw X-coordinate.
	eeShared, err := bip324EllswiftXDH(theirEphemeral, ourEphemeralEllSwift, ourEphemeralPriv)
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: ee ECDH: %w", err)
	}
	if err := hs.mixKey(eeShared[:]); err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: mixKey(ee): %w", err)
	}

	// Step 5: append EncryptAndHash(s.public_key) — our STATIC key, EllSwift-
	// encoded (64B), encrypted (+16B MAC) = 80 bytes total.
	_, ourStaticEllSwift, err := staticKP.ellswiftKeyMaterial()
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: static ellswift material: %w", err)
	}
	encStaticPub, err := hs.encryptAndHash(ourStaticEllSwift[:])
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: encrypt static pubkey: %w", err)
	}
	out = append(out, encStaticPub...)

	// Step 6: MixKey(ECDH(s.private_key, re.public_key)) — static-ephemeral DH.
	esShared, err := bip324EllswiftXDH(theirEphemeral, ourStaticEllSwift, staticKP.priv)
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: es ECDH: %w", err)
	}
	if err := hs.mixKey(esShared[:]); err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: mixKey(es): %w", err)
	}

	// Step 7: append EncryptAndHash(SIGNATURE_NOISE_MESSAGE) — 74B cert + 16B MAC = 90 bytes.
	now := uint32(time.Now().Unix())
	const certValiditySeconds = 60 * 60 * 24 // 24h, matches GSS's default cert validity
	sigMsg, err := buildSignatureNoiseMessage(authKP, staticKP.xOnlyPubKeyBytes(), now, now+certValiditySeconds)
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: build certificate: %w", err)
	}
	encSig, err := hs.encryptAndHash(sigMsg[:])
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: encrypt certificate: %w", err)
	}
	out = append(out, encSig...)

	if len(out) != initiatorExpectedHandshakeMsgSize {
		return nil, nil, fmt.Errorf("sv2 handshake: internal error, response size %d != expected %d",
			len(out), initiatorExpectedHandshakeMsgSize)
	}

	if _, err := conn.Write(out); err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: send response: %w", err)
	}

	// Step 9: derive transport ciphers. (temp_k1, temp_k2) = hkdf_2(ck, []).
	// From the RESPONDER's perspective: c1 decrypts initiator->responder
	// traffic (recv), c2 encrypts responder->initiator traffic (send).
	tempK1, tempK2 := hkdf2(hs.ck, nil)
	recvCipher, err := newSV2TransportCipher(tempK1)
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: init recv transport cipher: %w", err)
	}
	sendCipher, err := newSV2TransportCipher(tempK2)
	if err != nil {
		return nil, nil, fmt.Errorf("sv2 handshake: init send transport cipher: %w", err)
	}

	return sendCipher, recvCipher, nil
}
