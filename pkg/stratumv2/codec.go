package stratumv2

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// ──────────────────────────────────────────────────────────────────────────────
// SV2 Binary Frame Format
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
// ──────────────────────────────────────────────────────────────────────────────

const (
	// frameHeaderLen is the fixed size of a SV2 frame header.
	frameHeaderLen = 6

	// MaxFramePayload is the maximum allowed payload length (2^24 - 1 bytes).
	MaxFramePayload = 1<<24 - 1

	// ExtensionTypeMining is the extension_type for the Mining Protocol.
	ExtensionTypeMining uint16 = 0x0000
)

// Frame is a decoded SV2 frame ready for message parsing.
type Frame struct {
	ExtensionType uint16
	MsgType       uint8
	Payload       []byte
}

// Codec handles reading and writing SV2 binary frames over a net.Conn.
// The conn is expected to already be a Noise-encrypted connection after
// the handshake, but Codec works over any net.Conn.
type Codec struct {
	conn net.Conn
}

// NewCodec wraps conn in a SV2 frame codec.
func NewCodec(conn net.Conn) *Codec {
	return &Codec{conn: conn}
}

// ReadFrame reads exactly one SV2 frame from the connection.
// It blocks until a complete frame is available or an error occurs.
func (c *Codec) ReadFrame() (*Frame, error) {
	// Read the 6-byte header.
	hdr := make([]byte, frameHeaderLen)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return nil, fmt.Errorf("sv2 read header: %w", err)
	}

	extType := binary.LittleEndian.Uint16(hdr[0:2])
	msgType := hdr[2]

	// msg_length is a 24-bit LE value across bytes 3-5.
	payLen := uint32(hdr[3]) | uint32(hdr[4])<<8 | uint32(hdr[5])<<16

	if payLen > MaxFramePayload {
		return nil, fmt.Errorf("sv2 frame payload too large: %d bytes", payLen)
	}

	payload := make([]byte, payLen)
	if payLen > 0 {
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return nil, fmt.Errorf("sv2 read payload: %w", err)
		}
	}

	return &Frame{
		ExtensionType: extType,
		MsgType:       msgType,
		Payload:       payload,
	}, nil
}

// WriteFrame encodes and sends a SV2 frame.
// extType should be ExtensionTypeMining (0) for all standard mining messages.
func (c *Codec) WriteFrame(extType uint16, msgType uint8, payload []byte) error {
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("sv2 write: payload %d bytes exceeds 24-bit limit", len(payload))
	}

	hdr := make([]byte, frameHeaderLen)
	binary.LittleEndian.PutUint16(hdr[0:2], extType)
	hdr[2] = msgType

	// Encode payload length as 24-bit LE.
	payLen := len(payload)
	hdr[3] = byte(payLen)
	hdr[4] = byte(payLen >> 8)
	hdr[5] = byte(payLen >> 16)

	// Write header + payload as a single syscall to minimise fragmentation.
	pkt := make([]byte, frameHeaderLen+len(payload))
	copy(pkt, hdr)
	copy(pkt[frameHeaderLen:], payload)

	_, err := c.conn.Write(pkt)
	return err
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
