package stratumv2

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// ──────────────────────────────────────────────────────────────────────────────
// SV2 Binary Frame Format (plaintext, as seen after decryption)
//
//  0        1        2        3        4        5        6
//  ┌────────┬────────┬────────┬────────┬────────┬────────┬──────────────────┐
//  │  ext_type (u16) │msg_type│          msg_length (u24)  │  payload ...   │
//  └────────┴────────┴────────┴────────┴────────┴────────┴──────────────────┘
//  bytes:   0-1      2        3-5                           6..
//
//  ext_type:    16-bit LE — 0x0000 for the Mining Protocol; the MSB is the
//               "extension" flag.  ForgeNX only uses the Mining Protocol, so
//               this is always 0.
//  msg_type:    1 byte — identifies the message (see messages.go constants).
//  msg_length:  24-bit LE unsigned int — payload byte count (NOT including
//               the 6-byte header).
//
// Total header size: 6 bytes.
// Maximum payload:   16,777,215 bytes (2^24 - 1). In practice messages are
//                    small (< 256 bytes for most control messages).
//
// ──────────────────────────────────────────────────────────────────────────────
// ENCRYPTED wire format (post-handshake, transport phase)
//
// Ported from sv2/codec-sv2/src/{encoder,decoder}.rs and
// sv2/framing-sv2/src/header.rs. CRITICAL: there is NO explicit length
// prefix on the wire for encrypted frames — sizes are derived from fixed
// constants and the decrypted header's length field:
//
//   1. Read exactly ENCRYPTED_SV2_FRAME_HEADER_SIZE (22 = 6 + 16 MAC) bytes.
//      AEAD-decrypt them (one AEAD call) to get the plaintext 6-byte header.
//   2. From the decrypted header's msg_length, compute encrypted_len:
//        payloadPerChunk = SV2_FRAME_CHUNK_SIZE(65535) - AEAD_MAC_LEN(16) = 65519
//        chunks = ceil(msg_length / payloadPerChunk)
//        encrypted_len = msg_length + chunks*16
//   3. Read exactly encrypted_len more bytes, and AEAD-decrypt them in
//      sequence in chunks of up to SV2_FRAME_CHUNK_SIZE ciphertext bytes
//      each (the last chunk may be shorter) — each chunk is a SEPARATE AEAD
//      call advancing the cipher's nonce by one.
//
// Writing mirrors this: encrypt the 6-byte header alone as one AEAD frame,
// then encrypt the payload in chunks of up to (65535-16)=65519 plaintext
// bytes each, each chunk becoming its own AEAD-sealed wire segment.
// ──────────────────────────────────────────────────────────────────────────────

const (
	// frameHeaderLen is the fixed size of a SV2 frame header.
	frameHeaderLen = 6

	// MaxFramePayload is the maximum allowed payload length (2^24 - 1 bytes).
	MaxFramePayload = 1<<24 - 1

	// ExtensionTypeMining is the extension_type for the Mining Protocol.
	ExtensionTypeMining uint16 = 0x0000

	// sv2FrameChunkSize and encryptedFrameHeaderSize per framing-sv2/src/lib.rs.
	sv2FrameChunkSize        = 65535
	encryptedFrameHeaderSize = frameHeaderLen + aeadMACLen    // 22
	payloadPerChunk          = sv2FrameChunkSize - aeadMACLen // 65519
)

// Frame is a decoded SV2 frame ready for message parsing.
type Frame struct {
	ExtensionType uint16
	MsgType       uint8
	Payload       []byte
}

// Codec handles reading and writing SV2 binary frames.
//
// Pre-handshake (handshake bytes only): Codec is unused — the handshake is
// handled entirely by PerformSV2ServerHandshake in sv2noise.go, operating
// directly on the raw net.Conn.
//
// Post-handshake (transport phase): Codec wraps the raw net.Conn PLUS the
// two transport-phase AEAD ciphers, and implements the real chunked,
// length-prefix-free encrypted framing described above. This requires
// direct cipher access (not just an opaque net.Conn) because decrypting the
// header is what reveals how many more bytes to read for the payload.
type Codec struct {
	conn net.Conn
	send *sv2TransportCipher // encrypts frames we write (nil = read-only/no transport keys)
	recv *sv2TransportCipher // decrypts frames we read (nil = write-only/no transport keys)
}

// NewCodec wraps conn in a SV2 frame codec with the given transport-phase
// ciphers. send/recv come from PerformSV2ServerHandshake's return value.
func NewCodec(conn net.Conn, send, recv *sv2TransportCipher) *Codec {
	return &Codec{conn: conn, send: send, recv: recv}
}

// ReadFrame reads, decrypts, and decodes exactly one SV2 frame.
// It blocks until a complete frame is available or an error occurs.
func (c *Codec) ReadFrame() (*Frame, error) {
	if c.recv == nil {
		return nil, fmt.Errorf("sv2 codec: no recv cipher configured")
	}

	// Step 1: read + decrypt the fixed-size encrypted header (22 bytes).
	encHdr := make([]byte, encryptedFrameHeaderSize)
	if _, err := io.ReadFull(c.conn, encHdr); err != nil {
		return nil, fmt.Errorf("sv2 read encrypted header: %w", err)
	}
	hdr, err := c.recv.open(encHdr)
	if err != nil {
		return nil, fmt.Errorf("sv2 decrypt header: %w", err)
	}
	if len(hdr) != frameHeaderLen {
		return nil, fmt.Errorf("sv2 decrypted header: expected %d bytes, got %d", frameHeaderLen, len(hdr))
	}

	extType := binary.LittleEndian.Uint16(hdr[0:2])
	msgType := hdr[2]
	payLen := uint32(hdr[3]) | uint32(hdr[4])<<8 | uint32(hdr[5])<<16

	if payLen > MaxFramePayload {
		return nil, fmt.Errorf("sv2 frame payload too large: %d bytes", payLen)
	}

	if payLen == 0 {
		return &Frame{ExtensionType: extType, MsgType: msgType, Payload: nil}, nil
	}

	// Step 2: compute encrypted_len (payload + one 16-byte MAC per chunk),
	// then read and decrypt that many bytes in chunks.
	chunks := (int(payLen) + payloadPerChunk - 1) / payloadPerChunk
	encryptedLen := int(payLen) + chunks*aeadMACLen

	encPayload := make([]byte, encryptedLen)
	if _, err := io.ReadFull(c.conn, encPayload); err != nil {
		return nil, fmt.Errorf("sv2 read encrypted payload: %w", err)
	}

	payload := make([]byte, 0, payLen)
	start := 0
	for start < len(encPayload) {
		end := start + sv2FrameChunkSize
		if end > len(encPayload) {
			end = len(encPayload)
		}
		chunkPlain, err := c.recv.open(encPayload[start:end])
		if err != nil {
			return nil, fmt.Errorf("sv2 decrypt payload chunk: %w", err)
		}
		payload = append(payload, chunkPlain...)
		start = end
	}

	if len(payload) != int(payLen) {
		return nil, fmt.Errorf("sv2 decrypted payload: expected %d bytes, got %d", payLen, len(payload))
	}

	return &Frame{
		ExtensionType: extType,
		MsgType:       msgType,
		Payload:       payload,
	}, nil
}

// WriteFrame encodes, encrypts, and sends a SV2 frame.
// extType should be ExtensionTypeMining (0) for all standard mining messages.
func (c *Codec) WriteFrame(extType uint16, msgType uint8, payload []byte) error {
	if c.send == nil {
		return fmt.Errorf("sv2 codec: no send cipher configured")
	}
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("sv2 write: payload %d bytes exceeds 24-bit limit", len(payload))
	}

	hdr := make([]byte, frameHeaderLen)
	binary.LittleEndian.PutUint16(hdr[0:2], extType)
	hdr[2] = msgType

	payLen := len(payload)
	hdr[3] = byte(payLen)
	hdr[4] = byte(payLen >> 8)
	hdr[5] = byte(payLen >> 16)

	// Step 1: encrypt and send the 6-byte header as its own AEAD frame.
	encHdr := c.send.seal(hdr)
	if _, err := c.conn.Write(encHdr); err != nil {
		return fmt.Errorf("sv2 write encrypted header: %w", err)
	}

	// Step 2: encrypt and send the payload in chunks of up to 65519 plaintext
	// bytes each, writing each chunk's ciphertext immediately.
	start := 0
	for start < len(payload) {
		end := start + payloadPerChunk
		if end > len(payload) {
			end = len(payload)
		}
		encChunk := c.send.seal(payload[start:end])
		if _, err := c.conn.Write(encChunk); err != nil {
			return fmt.Errorf("sv2 write encrypted payload chunk: %w", err)
		}
		start = end
	}

	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Wire type helpers
//
// SV2 uses a strict set of binary primitive types.  All multi-byte integers
// are little-endian.  Strings are length-prefixed with a U8 or U16 count.
// ──────────────────────────────────────────────────────────────────────────────

// encodeSTR0_255 encodes a string as U8-length-prefixed bytes (max 255 chars).
func encodeSTR0_255(s string) ([]byte, error) {
	if len(s) > 255 {
		return nil, fmt.Errorf("sv2 STR0_255: string too long (%d bytes)", len(s))
	}
	b := make([]byte, 1+len(s))
	b[0] = byte(len(s))
	copy(b[1:], s)
	return b, nil
}

// decodeSTR0_255 reads a U8-length-prefixed string from buf at offset.
// Returns the string and the number of bytes consumed.
func decodeSTR0_255(buf []byte, offset int) (string, int, error) {
	if offset >= len(buf) {
		return "", 0, fmt.Errorf("sv2 STR0_255: offset %d out of range", offset)
	}
	slen := int(buf[offset])
	end := offset + 1 + slen
	if end > len(buf) {
		return "", 0, fmt.Errorf("sv2 STR0_255: string extends past buffer")
	}
	return string(buf[offset+1 : end]), slen + 1, nil
}

// encodeB0_32 encodes a byte slice with a U8 length prefix (max 32 bytes).
func encodeB0_32(b []byte) ([]byte, error) {
	if len(b) > 32 {
		return nil, fmt.Errorf("sv2 B0_32: too long (%d bytes)", len(b))
	}
	out := make([]byte, 1+len(b))
	out[0] = byte(len(b))
	copy(out[1:], b)
	return out, nil
}

// decodeB0_32 reads a U8-length-prefixed byte slice from buf at offset.
func decodeB0_32(buf []byte, offset int) ([]byte, int, error) {
	if offset >= len(buf) {
		return nil, 0, fmt.Errorf("sv2 B0_32: offset %d out of range", offset)
	}
	blen := int(buf[offset])
	if blen > 32 {
		return nil, 0, fmt.Errorf("sv2 B0_32: length %d exceeds 32", blen)
	}
	end := offset + 1 + blen
	if end > len(buf) {
		return nil, 0, fmt.Errorf("sv2 B0_32: slice extends past buffer")
	}
	out := make([]byte, blen)
	copy(out, buf[offset+1:end])
	return out, blen + 1, nil
}

// encodeU256 encodes a 256-bit value as exactly 32 raw bytes, NO length
// prefix — per sv2-spec 03-Protocol-Overview.md's data type table, U256 is
// "Unsigned integer, 256-bit, little-endian", distinct from B0_32 (which DOES
// have a length prefix). Every target/hash field in the Mining Protocol
// (max_target, target, maximum_target, prev_hash, merkle_root, etc.) is
// U256, not B0_32 — conflating the two was a real bug found via live testing
// against real Bitaxe Gamma SV2 firmware (decode failed with
// "B0_32: length 255 exceeds 32" because the first byte of a genuine 256-bit
// value was being misread as a length prefix).
func encodeU256(b []byte) ([]byte, error) {
	if len(b) != 32 {
		return nil, fmt.Errorf("sv2 U256: must be exactly 32 bytes, got %d", len(b))
	}
	out := make([]byte, 32)
	copy(out, b)
	return out, nil
}

// decodeU256 reads exactly 32 raw bytes from buf at offset (no length prefix).
func decodeU256(buf []byte, offset int) ([]byte, int, error) {
	end := offset + 32
	if end > len(buf) {
		return nil, 0, fmt.Errorf("sv2 U256: need 32 bytes at offset %d, buffer has %d", offset, len(buf)-offset)
	}
	out := make([]byte, 32)
	copy(out, buf[offset:end])
	return out, 32, nil
}

// putU16LE writes a uint16 in LE at buf[offset].
func putU16LE(buf []byte, offset int, v uint16) {
	binary.LittleEndian.PutUint16(buf[offset:], v)
}

// putU32LE writes a uint32 in LE at buf[offset].
func putU32LE(buf []byte, offset int, v uint32) {
	binary.LittleEndian.PutUint32(buf[offset:], v)
}

// putU64LE writes a uint64 in LE at buf[offset].
func putU64LE(buf []byte, offset int, v uint64) {
	binary.LittleEndian.PutUint64(buf[offset:], v)
}

func getU16LE(buf []byte, offset int) uint16 {
	return binary.LittleEndian.Uint16(buf[offset:])
}

func getU32LE(buf []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(buf[offset:])
}

func getU64LE(buf []byte, offset int) uint64 {
	return binary.LittleEndian.Uint64(buf[offset:])
}
