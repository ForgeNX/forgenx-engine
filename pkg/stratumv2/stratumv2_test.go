package stratumv2

import (
	"bytes"
	"fmt"
	"math/big"
	"net"
	"testing"
)

// TestMessageTypeConstants pins every message type byte to the authoritative
// values from sv2-spec's 08-Message-Types.md table. An earlier version of
// this package had every constant from MsgNewMiningJob onward wrong (off by
// varying amounts, not a single consistent offset) — confirmed via live
// Bitaxe Gamma testing, where the firmware logged "Unknown SV2 message type:
// 0x1a" because we'd sent NewMiningJob using 0x1A, which is actually
// SubmitSharesStandard in the real spec. This test exists specifically so
// that class of silent renumbering bug can't recur undetected.
func TestMessageTypeConstants(t *testing.T) {
	cases := []struct {
		name string
		got  uint8
		want uint8
	}{
		{"MsgSetupConnection", MsgSetupConnection, 0x00},
		{"MsgSetupConnectionSuccess", MsgSetupConnectionSuccess, 0x01},
		{"MsgSetupConnectionError", MsgSetupConnectionError, 0x02},
		{"MsgOpenStandardMiningChannel", MsgOpenStandardMiningChannel, 0x10},
		{"MsgOpenStandardMiningChannelSuccess", MsgOpenStandardMiningChannelSuccess, 0x11},
		{"MsgOpenStandardMiningChannelError", MsgOpenStandardMiningChannelError, 0x12},
		{"MsgNewMiningJob", MsgNewMiningJob, 0x15},
		{"MsgSubmitSharesStandard", MsgSubmitSharesStandard, 0x1A},
		{"MsgSubmitSharesSuccess", MsgSubmitSharesSuccess, 0x1C},
		{"MsgSubmitSharesError", MsgSubmitSharesError, 0x1D},
		{"MsgSetNewPrevHash", MsgSetNewPrevHash, 0x20},
		{"MsgSetTarget", MsgSetTarget, 0x21},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: want 0x%02X, got 0x%02X", c.name, c.want, c.got)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// codec_test
//
// Tests exercise the REAL encrypted SV2 wire framing (header-then-chunked-
// payload, no length prefix — see codec.go's header comment). Each test
// builds two Codecs sharing a net.Pipe(), with cipher keys cross-wired so
// side A's send cipher matches side B's recv cipher and vice versa — exactly
// how PerformSV2ServerHandshake's (send, recv) pair works in practice.
// ──────────────────────────────────────────────────────────────────────────────

// newTestCodecPair builds two Codecs over a connected net.Pipe(), with
// matching transport ciphers so each side can decrypt what the other sends.
func newTestCodecPair(t *testing.T) (*Codec, *Codec) {
	t.Helper()

	connA, connB := net.Pipe()

	var keyAtoB, keyBtoA [32]byte
	keyAtoB[0] = 0x01 // distinct keys per direction, matching real handshake behavior
	keyBtoA[0] = 0x02

	aSend, err := newSV2TransportCipher(keyAtoB)
	if err != nil {
		t.Fatalf("newSV2TransportCipher aSend: %v", err)
	}
	aRecv, err := newSV2TransportCipher(keyBtoA)
	if err != nil {
		t.Fatalf("newSV2TransportCipher aRecv: %v", err)
	}
	bSend, err := newSV2TransportCipher(keyBtoA)
	if err != nil {
		t.Fatalf("newSV2TransportCipher bSend: %v", err)
	}
	bRecv, err := newSV2TransportCipher(keyAtoB)
	if err != nil {
		t.Fatalf("newSV2TransportCipher bRecv: %v", err)
	}

	ca := NewCodec(connA, aSend, aRecv)
	cb := NewCodec(connB, bSend, bRecv)
	return ca, cb
}

func TestCodecRoundtrip(t *testing.T) {
	ca, cb := newTestCodecPair(t)

	payload := []byte("hello sv2")
	done := make(chan error, 1)

	go func() {
		frame, err := cb.ReadFrame()
		if err != nil {
			done <- err
			return
		}
		if frame.MsgType != MsgNewMiningJob {
			t.Errorf("expected MsgType MsgNewMiningJob (0x%02X), got 0x%02X", MsgNewMiningJob, frame.MsgType)
		}
		if !bytes.Equal(frame.Payload, payload) {
			t.Errorf("payload mismatch: got %q, want %q", frame.Payload, payload)
		}
		done <- nil
	}()

	if err := ca.WriteFrame(ExtensionTypeMining, MsgNewMiningJob, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
}

func TestCodecEmptyPayload(t *testing.T) {
	ca, cb := newTestCodecPair(t)

	done := make(chan error, 1)
	go func() {
		frame, err := cb.ReadFrame()
		if err != nil {
			done <- err
			return
		}
		if len(frame.Payload) != 0 {
			t.Errorf("expected empty payload, got %d bytes", len(frame.Payload))
		}
		done <- nil
	}()

	if err := ca.WriteFrame(ExtensionTypeMining, MsgSetupConnectionSuccess, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
}

// TestCodecMultiChunkPayload exercises the chunking logic (payloadPerChunk =
// 65519 bytes) with a payload large enough to require two chunks — the exact
// edge case that the original framing bug (no chunking at all) would have
// silently mishandled.
func TestCodecMultiChunkPayload(t *testing.T) {
	ca, cb := newTestCodecPair(t)

	payload := make([]byte, payloadPerChunk+1000) // forces exactly 2 chunks
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	done := make(chan error, 1)
	go func() {
		frame, err := cb.ReadFrame()
		if err != nil {
			done <- err
			return
		}
		if !bytes.Equal(frame.Payload, payload) {
			done <- fmt.Errorf("payload mismatch: got %d bytes, want %d bytes", len(frame.Payload), len(payload))
			return
		}
		done <- nil
	}()

	if err := ca.WriteFrame(ExtensionTypeMining, MsgNewMiningJob, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// messages_test
// ──────────────────────────────────────────────────────────────────────────────

func TestSetupConnectionSuccess(t *testing.T) {
	payload := EncodeSetupConnectionSuccess(2, sv2ServerResponseFlags)
	if len(payload) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(payload))
	}
	version := getU16LE(payload, 0)
	flags := getU32LE(payload, 2)
	if version != 2 {
		t.Errorf("version: want 2, got %d", version)
	}
	if flags != sv2ServerResponseFlags {
		t.Errorf("flags: want %08X, got %08X", sv2ServerResponseFlags, flags)
	}
}

// TestEncodeNewMiningJob verifies the Standard Channel job wire format —
// channel_id(4) + job_id(4) + min_ntime(OPTION[u32]) + version(4) +
// merkle_root(U256, 32). An earlier version sent the Extended Channel
// format (raw coinbase + merkle branch) to Standard Channel clients;
// confirmed via live Bitaxe Gamma testing that this caused the firmware to
// silently fail to queue work, despite no parse error being raised.
func TestEncodeNewMiningJob(t *testing.T) {
	var merkleRoot [32]byte
	merkleRoot[0] = 0xCD
	merkleRoot[31] = 0xEF

	t.Run("future job (min_ntime unset)", func(t *testing.T) {
		payload := EncodeNewMiningJob(1, 5, false, 0, 0x20000000, merkleRoot)
		wantLen := 4 + 4 + 1 + 4 + 32
		if len(payload) != wantLen {
			t.Fatalf("payload length: want %d, got %d", wantLen, len(payload))
		}
		if got := getU32LE(payload, 0); got != 1 {
			t.Errorf("channel_id: want 1, got %d", got)
		}
		if got := getU32LE(payload, 4); got != 5 {
			t.Errorf("job_id: want 5, got %d", got)
		}
		if payload[8] != 0 {
			t.Errorf("min_ntime presence byte: want 0 (unset), got %d", payload[8])
		}
		if got := getU32LE(payload, 9); got != 0x20000000 {
			t.Errorf("version: want 0x20000000, got 0x%08X", got)
		}
		if !bytes.Equal(payload[13:45], merkleRoot[:]) {
			t.Errorf("merkle_root mismatch")
		}
	})

	t.Run("active job (min_ntime set)", func(t *testing.T) {
		payload := EncodeNewMiningJob(1, 5, true, 1700000000, 0x20000000, merkleRoot)
		wantLen := 4 + 4 + 1 + 4 + 4 + 32
		if len(payload) != wantLen {
			t.Fatalf("payload length: want %d, got %d", wantLen, len(payload))
		}
		if payload[8] != 1 {
			t.Errorf("min_ntime presence byte: want 1 (set), got %d", payload[8])
		}
		if got := getU32LE(payload, 9); got != 1700000000 {
			t.Errorf("min_ntime value: want 1700000000, got %d", got)
		}
		if got := getU32LE(payload, 13); got != 0x20000000 {
			t.Errorf("version: want 0x20000000, got 0x%08X", got)
		}
		if !bytes.Equal(payload[17:49], merkleRoot[:]) {
			t.Errorf("merkle_root mismatch")
		}
	})
}

func TestEncodeDecodeSubmitSharesStandard(t *testing.T) {
	// Build a raw payload manually and decode it.
	payload := make([]byte, 24)
	putU32LE(payload, 0, 42)          // channelID
	putU32LE(payload, 4, 7)           // seqNum
	putU32LE(payload, 8, 3)           // jobID
	putU32LE(payload, 12, 0xDEADBEEF) // nonce
	putU32LE(payload, 16, 1700000000) // nTime
	putU32LE(payload, 20, 0x20000000) // version

	share, err := DecodeSubmitSharesStandard(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if share.ChannelID != 42 {
		t.Errorf("channelID: want 42, got %d", share.ChannelID)
	}
	if share.Nonce != 0xDEADBEEF {
		t.Errorf("nonce: want 0xDEADBEEF, got 0x%08X", share.Nonce)
	}
}

func TestEncodeDecodeOpenStandardMiningChannel(t *testing.T) {
	// Construct a minimal valid payload.
	// requestID(4) + userIdentity(STR0_255) + hashrate(4 float32) + maxTarget(U256, 32 raw bytes, no length prefix)
	var buf []byte
	buf = appendU32LE(buf, 1) // requestID

	// user identity: "worker1"
	ui := "worker1"
	buf = append(buf, byte(len(ui)))
	buf = append(buf, []byte(ui)...)

	// nominal hashrate: 0 (valid float32 zero)
	buf = appendU32LE(buf, 0)

	// maxTarget: U256 is exactly 32 raw bytes, NO length prefix byte
	buf = append(buf, make([]byte, 32)...)

	msg, err := DecodeOpenStandardMiningChannel(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.RequestID != 1 {
		t.Errorf("requestID: want 1, got %d", msg.RequestID)
	}
	if msg.UserIdentity != "worker1" {
		t.Errorf("userIdentity: want %q, got %q", "worker1", msg.UserIdentity)
	}
}

func appendU32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// TestEncodeOpenStandardMiningChannelSuccess catches the field-layout bug
// found via live Bitaxe testing: an earlier version sent a fabricated
// extranonce2size uint16 instead of the real spec field (extranonce_prefix,
// B0_32), and encoded target as B0_32 instead of U256 (fixed 32 bytes, no
// length prefix). This verifies the wire layout matches sv2-spec
// 05-Mining-Protocol.md §5.3.3 exactly: request_id(4) + channel_id(4) +
// target(32, no prefix) + extranonce_prefix(B0_32) + group_channel_id(4).
func TestEncodeOpenStandardMiningChannelSuccess(t *testing.T) {
	target := make([]byte, 32)
	target[0] = 0xAB // distinctive byte to catch any offset errors
	extranoncePrefix := []byte{0x01, 0x02, 0x03, 0x04}

	payload, err := EncodeOpenStandardMiningChannelSuccess(42, 7, target, extranoncePrefix, 99)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	wantLen := 4 + 4 + 32 + (1 + len(extranoncePrefix)) + 4
	if len(payload) != wantLen {
		t.Fatalf("payload length: want %d, got %d", wantLen, len(payload))
	}

	if got := getU32LE(payload, 0); got != 42 {
		t.Errorf("request_id: want 42, got %d", got)
	}
	if got := getU32LE(payload, 4); got != 7 {
		t.Errorf("channel_id: want 7, got %d", got)
	}
	// target: 32 raw bytes starting at offset 8, NO length prefix byte.
	if payload[8] != 0xAB {
		t.Errorf("target[0]: want 0xAB, got 0x%02X — possible length-prefix offset bug", payload[8])
	}
	// extranonce_prefix: B0_32, starts at offset 8+32=40, length byte then data.
	prefixOff := 40
	if payload[prefixOff] != byte(len(extranoncePrefix)) {
		t.Errorf("extranonce_prefix length byte: want %d, got %d", len(extranoncePrefix), payload[prefixOff])
	}
	if !bytes.Equal(payload[prefixOff+1:prefixOff+1+len(extranoncePrefix)], extranoncePrefix) {
		t.Errorf("extranonce_prefix bytes mismatch")
	}
	// group_channel_id: last 4 bytes.
	groupOff := prefixOff + 1 + len(extranoncePrefix)
	if got := getU32LE(payload, groupOff); got != 99 {
		t.Errorf("group_channel_id: want 99, got %d", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// pow_test
// ──────────────────────────────────────────────────────────────────────────────

func TestNBitsToTarget(t *testing.T) {
	// Bitcoin genesis block nBits = 0x1d00ffff
	// Expected target: 0x00000000FFFF0000...0000
	nBits := uint32(0x1d00ffff)
	target := NBitsToTarget(nBits)

	expected, ok := new(big.Int).SetString("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
	if !ok {
		t.Fatal("failed to parse expected target")
	}
	if target.Cmp(expected) != 0 {
		t.Errorf("nBits 0x%08X: got %X, want %X", nBits, target, expected)
	}
}

func TestTargetRoundtrip(t *testing.T) {
	// Target → bytes → Target should be identity.
	original, _ := new(big.Int).SetString("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
	b := TargetToBytes(original)
	recovered := BytesToTarget(b)
	if original.Cmp(recovered) != 0 {
		t.Errorf("roundtrip failed: got %X, want %X", recovered, original)
	}
}

func TestDifficultyConversion(t *testing.T) {
	// diff1 target / diff1 target should be ~1.0
	target := DifficultyToTarget(1.0)
	diff := TargetToDifficulty(target)
	if diff < 0.9999 || diff > 1.0001 {
		t.Errorf("diff1 roundtrip: got %.6f, want ~1.0", diff)
	}

	// Difficulty 512 should give a target 512x harder (smaller).
	t512 := DifficultyToTarget(512.0)
	if t512.Cmp(target) >= 0 {
		t.Error("difficulty 512 target should be smaller than difficulty 1 target")
	}
}

func TestHashBlockHeader(t *testing.T) {
	// Bitcoin genesis block header (80 bytes), well-known hash.
	// We just check the function runs and returns 32 bytes.
	var hdr [80]byte
	hash := HashBlockHeader(hdr)
	if len(hash) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(hash))
	}
}

func TestComputeMerkleRoot_SingleCoinbase(t *testing.T) {
	// With an empty branch, the Merkle root equals the coinbase tx hash.
	var coinbaseTxHash [32]byte
	for i := range coinbaseTxHash {
		coinbaseTxHash[i] = byte(i)
	}
	root := ComputeMerkleRoot(coinbaseTxHash, nil)
	if root != coinbaseTxHash {
		t.Error("empty branch: merkle root should equal coinbase hash")
	}
}

func TestPrevHashFromHex(t *testing.T) {
	// Round-trip: a known display-order hash reversed to internal order.
	displayHex := "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	internal, err := PrevHashFromHex(displayHex)
	if err != nil {
		t.Fatalf("PrevHashFromHex: %v", err)
	}
	// Internal byte 0 should be the last byte of the display hex.
	if internal[0] != 0x6f {
		t.Errorf("internal[0]: want 0x6f, got 0x%02X", internal[0])
	}
	if internal[31] != 0x00 {
		t.Errorf("internal[31]: want 0x00, got 0x%02X", internal[31])
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// channel_test
// ──────────────────────────────────────────────────────────────────────────────

func TestChannelShareAccounting(t *testing.T) {
	ch := &Channel{
		id:              1,
		poolTargetBytes: TargetToBytes(DifficultyToTarget(512)),
		poolDifficulty:  512,
	}

	lastSeq, accepted, sumDiff, _ := ch.RecordShare(1, 512.0, 1000.0, 800000)
	if lastSeq != 1 {
		t.Errorf("lastSeq: want 1, got %d", lastSeq)
	}
	if accepted != 1 {
		t.Errorf("accepted: want 1, got %d", accepted)
	}
	if sumDiff == 0 {
		t.Error("sumDiff should be non-zero")
	}

	a, r, _ := ch.Stats()
	if a != 1 {
		t.Errorf("stats accepted: want 1, got %d", a)
	}
	if r != 0 {
		t.Errorf("stats rejected: want 0, got %d", r)
	}
}

func TestChannelJobValidation(t *testing.T) {
	ch := &Channel{id: 1}
	ch.SetCurrentJob(10)
	ch.SetCurrentJob(11)
	ch.SetCurrentJob(12)

	if !ch.IsJobValid(12) {
		t.Error("current job 12 should be valid")
	}
	if !ch.IsJobValid(11) {
		t.Error("stale job 11 should be valid")
	}
	if !ch.IsJobValid(10) {
		t.Error("stale job 10 should be valid (within 2-deep stale window)")
	}
	if ch.IsJobValid(9) {
		t.Error("job 9 (too old) should not be valid")
	}
}

func TestChannelSetPoolDifficulty(t *testing.T) {
	ch := &Channel{
		id:              1,
		poolTargetBytes: TargetToBytes(DifficultyToTarget(DefaultPoolDifficulty)),
		poolDifficulty:  DefaultPoolDifficulty,
	}

	newTarget := ch.SetPoolDifficulty(1024.0)
	if len(newTarget) != 32 {
		t.Errorf("new target: expected 32 bytes, got %d", len(newTarget))
	}

	// Higher difficulty → smaller target.
	oldTargetInt := DifficultyToTarget(DefaultPoolDifficulty)
	newTargetInt := BytesToTarget(newTarget)
	if newTargetInt.Cmp(oldTargetInt) >= 0 {
		t.Error("difficulty 1024 target should be smaller than default difficulty target")
	}
}
