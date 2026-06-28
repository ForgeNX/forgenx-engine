package stratumv2

import (
	"bytes"
	"math/big"
	"net"
	"testing"
)

// ──────────────────────────────────────────────────────────────────────────────
// codec_test
// ──────────────────────────────────────────────────────────────────────────────

// fakeConn is a net.Conn backed by a bytes.Buffer for testing.
type fakeConn struct {
	net.Conn
	buf *bytes.Buffer
}

func newFakeConn() (*fakeConn, *fakeConn) {
	// Two connected fake conns sharing a pipe.
	r, w := net.Pipe()
	_ = r
	_ = w
	// Use net.Pipe() for real bidirectional testing.
	return &fakeConn{Conn: r}, &fakeConn{Conn: w}
}

func TestCodecRoundtrip(t *testing.T) {
	a, b := newFakeConn()
	ca := NewCodec(a)
	cb := NewCodec(b)

	payload := []byte("hello sv2")
	done := make(chan error, 1)

	go func() {
		frame, err := cb.ReadFrame()
		if err != nil {
			done <- err
			return
		}
		if frame.MsgType != 0x1A {
			t.Errorf("expected MsgType 0x1A, got 0x%02X", frame.MsgType)
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
	a, b := newFakeConn()
	ca := NewCodec(a)
	cb := NewCodec(b)

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

// ──────────────────────────────────────────────────────────────────────────────
// messages_test
// ──────────────────────────────────────────────────────────────────────────────

func TestSetupConnectionSuccess(t *testing.T) {
	payload := EncodeSetupConnectionSuccess(2, sv2FlagsSupported)
	if len(payload) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(payload))
	}
	version := getU16LE(payload, 0)
	flags := getU32LE(payload, 2)
	if version != 2 {
		t.Errorf("version: want 2, got %d", version)
	}
	if flags != sv2FlagsSupported {
		t.Errorf("flags: want %08X, got %08X", sv2FlagsSupported, flags)
	}
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
	// requestID(4) + userIdentity(STR0_255) + hashrate(4 float32) + maxTarget(B0_32)
	var buf []byte
	buf = appendU32LE(buf, 1) // requestID

	// user identity: "worker1"
	ui := "worker1"
	buf = append(buf, byte(len(ui)))
	buf = append(buf, []byte(ui)...)

	// nominal hashrate: 0 (valid float32 zero)
	buf = appendU32LE(buf, 0)

	// maxTarget: 32 zero bytes (easiest possible)
	buf = append(buf, 32) // B0_32 length prefix
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

	lastSeq, accepted, sumDiff := ch.RecordShare(1, 512.0)
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
