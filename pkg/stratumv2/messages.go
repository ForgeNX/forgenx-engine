package stratumv2

import (
	"fmt"
	"math"
)

// ──────────────────────────────────────────────────────────────────────────────
// SV2 Mining Protocol — Message Type Constants
//
// Full specification:
//   https://stratumprotocol.org/specification/06-Mining-Protocol/
//
// We implement the subset required for Standard Mining Channel:
//   SetupConnection / .Success / .Error
//   OpenStandardMiningChannel / .Success / .Error
//   NewMiningJob
//   SetNewPrevHash
//   SubmitSharesStandard / .Success / .Error
//   SetTarget
// ──────────────────────────────────────────────────────────────────────────────

// Mining Protocol message types (msg_type byte).
const (
	MsgSetupConnection                  uint8 = 0x00
	MsgSetupConnectionSuccess           uint8 = 0x01
	MsgSetupConnectionError             uint8 = 0x02
	MsgOpenStandardMiningChannel        uint8 = 0x10
	MsgOpenStandardMiningChannelSuccess uint8 = 0x11
	MsgOpenStandardMiningChannelError   uint8 = 0x12
	MsgOpenExtendedMiningChannel        uint8 = 0x13
	MsgOpenExtendedMiningChannelSuccess uint8 = 0x14
	MsgNewMiningJob                     uint8 = 0x15
	MsgSetNewPrevHash                   uint8 = 0x20
	MsgSubmitSharesStandard             uint8 = 0x1A
	MsgSubmitSharesSuccess              uint8 = 0x1C
	MsgSubmitSharesError                uint8 = 0x1D
	MsgSubmitSharesExtended             uint8 = 0x1E
	MsgNewExtendedMiningJob             uint8 = 0x1F
	MsgSetTarget                        uint8 = 0x21
)

// SetupConnection error codes.
const (
	ErrUnsupportedFeatureFlags uint16 = 0x0001
	ErrUnsupportedProtocol     uint16 = 0x0002
	ErrProtocolVersionMismatch uint16 = 0x0003
)

// Protocol identifies which SV2 sub-protocol the client wants.
type Protocol uint8

const (
	ProtocolMining          Protocol = 0
	ProtocolJobDeclaration  Protocol = 1
	ProtocolTemplateDistrib Protocol = 2
)

// ──────────────────────────────────────────────────────────────────────────────
// SetupConnection (client → server)
// ──────────────────────────────────────────────────────────────────────────────

// MsgSetupConnectionFields holds the decoded payload of a SetupConnection message.
type MsgSetupConnectionFields struct {
	Protocol        Protocol
	MinVersion      uint16
	MaxVersion      uint16
	Flags           uint32
	EndpointHost    string // STR0_255 — ASCII hostname or IP the client is connecting to
	EndpointPort    uint16 // connecting port value
	Vendor          string // STR0_255
	HardwareVersion string // STR0_255
	Firmware        string // STR0_255
	DeviceID        string // STR0_255
}

// DecodeSetupConnection parses a SetupConnection payload.
//
// Wire order per sv2-spec 03-Protocol-Overview.md §3.6.1:
//
//	protocol(1) + min_version(2) + max_version(2) + flags(4) +
//	endpoint_host(STR0_255) + endpoint_port(2) +
//	vendor(STR0_255) + hardware_version(STR0_255) + firmware(STR0_255) + device_id(STR0_255)
func DecodeSetupConnection(payload []byte) (*MsgSetupConnectionFields, error) {
	if len(payload) < 9 {
		return nil, errShort("SetupConnection", len(payload), 9)
	}
	m := &MsgSetupConnectionFields{}
	m.Protocol = Protocol(payload[0])
	m.MinVersion = getU16LE(payload, 1)
	m.MaxVersion = getU16LE(payload, 3)
	m.Flags = getU32LE(payload, 5)

	offset := 9
	var consumed int
	var err error

	if m.EndpointHost, consumed, err = decodeSTR0_255(payload, offset); err != nil {
		return nil, err
	}
	offset += consumed

	if offset+2 > len(payload) {
		return nil, errShort("SetupConnection endpoint_port", len(payload), offset+2)
	}
	m.EndpointPort = getU16LE(payload, offset)
	offset += 2

	if m.Vendor, consumed, err = decodeSTR0_255(payload, offset); err != nil {
		return nil, err
	}
	offset += consumed

	if m.HardwareVersion, consumed, err = decodeSTR0_255(payload, offset); err != nil {
		return nil, err
	}
	offset += consumed

	if m.Firmware, consumed, err = decodeSTR0_255(payload, offset); err != nil {
		return nil, err
	}
	offset += consumed

	if m.DeviceID, _, err = decodeSTR0_255(payload, offset); err != nil {
		return nil, err
	}

	return m, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SetupConnection.Success (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSetupConnectionSuccess builds the payload for a SetupConnection.Success.
func EncodeSetupConnectionSuccess(usedVersion uint16, flags uint32) []byte {
	b := make([]byte, 6)
	putU16LE(b, 0, usedVersion)
	putU32LE(b, 2, flags)
	return b
}

// ──────────────────────────────────────────────────────────────────────────────
// SetupConnection.Error (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSetupConnectionError builds a SetupConnection.Error payload.
func EncodeSetupConnectionError(flags uint32, errCode string) ([]byte, error) {
	codeBytes, err := encodeSTR0_255(errCode)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+len(codeBytes))
	putU32LE(b, 0, flags)
	copy(b[4:], codeBytes)
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenStandardMiningChannel (client → server)
// ──────────────────────────────────────────────────────────────────────────────

// MsgOpenStandardMiningChannelFields is the decoded payload.
type MsgOpenStandardMiningChannelFields struct {
	RequestID       uint32
	UserIdentity    string  // STR0_255 — miner's worker name / address
	NominalHashrate float32 // hashrate in H/s as IEEE-754 float32
	MaxTargetBytes  []byte  // U256 — maximum acceptable target (256-bit, LE, fixed 32 bytes, NO length prefix)
}

// DecodeOpenStandardMiningChannel parses the payload.
func DecodeOpenStandardMiningChannel(payload []byte) (*MsgOpenStandardMiningChannelFields, error) {
	if len(payload) < 9 {
		return nil, errShort("OpenStandardMiningChannel", len(payload), 9)
	}
	m := &MsgOpenStandardMiningChannelFields{}
	m.RequestID = getU32LE(payload, 0)

	var consumed int
	var err error
	m.UserIdentity, consumed, err = decodeSTR0_255(payload, 4)
	if err != nil {
		return nil, err
	}
	offset := 4 + consumed

	if offset+4 > len(payload) {
		return nil, errShort("OpenStandardMiningChannel nominal_hash_rate", len(payload), offset+4)
	}
	rawFloat := getU32LE(payload, offset)
	m.NominalHashrate = math.Float32frombits(rawFloat)
	offset += 4

	m.MaxTargetBytes, _, err = decodeU256(payload, offset)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenStandardMiningChannel.Success (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeOpenStandardMiningChannelSuccess builds the success response.
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.3:
//
//	request_id(4) + channel_id(4) + target(U256, 32) +
//	extranonce_prefix(B0_32) + group_channel_id(4)
func EncodeOpenStandardMiningChannelSuccess(
	requestID uint32,
	channelID uint32,
	targetBytes []byte,
	extranoncePrefix []byte,
	groupChannelID uint32,
) ([]byte, error) {
	targetEnc, err := encodeU256(targetBytes)
	if err != nil {
		return nil, err
	}
	prefixEnc, err := encodeB0_32(extranoncePrefix)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+4+len(targetEnc)+len(prefixEnc)+4)
	putU32LE(b, 0, requestID)
	putU32LE(b, 4, channelID)
	off := 8
	copy(b[off:], targetEnc)
	off += len(targetEnc)
	copy(b[off:], prefixEnc)
	off += len(prefixEnc)
	putU32LE(b, off, groupChannelID)
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenStandardMiningChannel.Error (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeOpenStandardMiningChannelError builds the error response.
func EncodeOpenStandardMiningChannelError(requestID uint32, errCode string) ([]byte, error) {
	codeBytes, err := encodeSTR0_255(errCode)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+len(codeBytes))
	putU32LE(b, 0, requestID)
	copy(b[4:], codeBytes)
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenExtendedMiningChannel (client → server)
//
// Sent instead of OpenStandardMiningChannel by clients that want to build
// their own coinbase / roll their own extranonce2 (e.g. NerdQAxe++ firmware,
// which unconditionally opens extended channels).
// ──────────────────────────────────────────────────────────────────────────────

// MsgOpenExtendedMiningChannelFields is the decoded payload.
type MsgOpenExtendedMiningChannelFields struct {
	RequestID         uint32
	UserIdentity      string  // STR0_255
	NominalHashrate   float32 // H/s, IEEE-754 float32
	MaxTargetBytes    []byte  // U256, 32 bytes, no length prefix
	MinExtranonceSize uint16  // miner's requested minimum extranonce2 space, in bytes
}

// DecodeOpenExtendedMiningChannel parses the payload.
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.2:
//
//	request_id(4) + user_identity(STR0_255) + nominal_hash_rate(4) +
//	max_target(U256, 32) + min_extranonce_size(2)
func DecodeOpenExtendedMiningChannel(payload []byte) (*MsgOpenExtendedMiningChannelFields, error) {
	if len(payload) < 9 {
		return nil, errShort("OpenExtendedMiningChannel", len(payload), 9)
	}
	m := &MsgOpenExtendedMiningChannelFields{}
	m.RequestID = getU32LE(payload, 0)

	var consumed int
	var err error
	m.UserIdentity, consumed, err = decodeSTR0_255(payload, 4)
	if err != nil {
		return nil, err
	}
	offset := 4 + consumed

	if offset+4 > len(payload) {
		return nil, errShort("OpenExtendedMiningChannel nominal_hash_rate", len(payload), offset+4)
	}
	rawFloat := getU32LE(payload, offset)
	m.NominalHashrate = math.Float32frombits(rawFloat)
	offset += 4

	m.MaxTargetBytes, consumed, err = decodeU256(payload, offset)
	if err != nil {
		return nil, err
	}
	offset += consumed

	if offset+2 > len(payload) {
		return nil, errShort("OpenExtendedMiningChannel min_extranonce_size", len(payload), offset+2)
	}
	m.MinExtranonceSize = getU16LE(payload, offset)
	return m, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenExtendedMiningChannel.Success (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeOpenExtendedMiningChannelSuccess builds the success response.
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.4:
//
//	request_id(4) + channel_id(4) + target(U256, 32) +
//	extranonce_size(2) + extranonce_prefix(B0_32)
//
// *** VERIFY ***: field order (extranonce_size before extranonce_prefix)
// matches the SRI reference implementation / spec doc, but has not been
// confirmed against a live capture of NerdQAxe++ traffic. If the device
// hangs or rejects after this response, capture the bytes it actually
// expects and swap the field order here.
func EncodeOpenExtendedMiningChannelSuccess(
	requestID uint32,
	channelID uint32,
	targetBytes []byte,
	extranonceSize uint16,
	extranoncePrefix []byte,
	groupChannelID uint32,
) ([]byte, error) {
	targetEnc, err := encodeU256(targetBytes)
	if err != nil {
		return nil, err
	}
	prefixEnc, err := encodeB0_32(extranoncePrefix)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+4+len(targetEnc)+2+len(prefixEnc)+4)
	putU32LE(b, 0, requestID)
	putU32LE(b, 4, channelID)
	off := 8
	copy(b[off:], targetEnc)
	off += len(targetEnc)
	putU16LE(b, off, extranonceSize)
	off += 2
	copy(b[off:], prefixEnc)
	off += len(prefixEnc)
	putU32LE(b, off, groupChannelID)
	return b, nil
}

// EncodeOpenExtendedMiningChannelError builds an OpenExtendedMiningChannel
// error response. Per spec, OpenMiningChannel.Error (msg 0x12) is shared
// between standard and extended channel opens — the wire format is
// identical (request_id + error_code) — so this just wraps the existing
// standard-channel encoder for call-site clarity.
func EncodeOpenExtendedMiningChannelError(requestID uint32, errCode string) ([]byte, error) {
	return EncodeOpenStandardMiningChannelError(requestID, errCode)
}

// ──────────────────────────────────────────────────────────────────────────────
// SetTarget (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSetTarget builds the payload for a SetTarget message.
func EncodeSetTarget(channelID uint32, targetBytes []byte) ([]byte, error) {
	targetEnc, err := encodeU256(targetBytes)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+len(targetEnc))
	putU32LE(b, 0, channelID)
	copy(b[4:], targetEnc)
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SetNewPrevHash (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSetNewPrevHash builds the payload.
// prevHash must be the 32-byte previous block hash in internal byte order (LE).
func EncodeSetNewPrevHash(
	channelID uint32,
	jobID uint32,
	prevHash [32]byte,
	minNtime uint32,
	nBits uint32,
) []byte {
	b := make([]byte, 4+4+32+4+4)
	putU32LE(b, 0, channelID)
	putU32LE(b, 4, jobID)
	copy(b[8:40], prevHash[:])
	putU32LE(b, 40, minNtime)
	putU32LE(b, 44, nBits)
	return b
}

// ──────────────────────────────────────────────────────────────────────────────
// NewMiningJob (server → client) — STANDARD CHANNEL format
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.15:
//
//	channel_id(4) + job_id(4) + min_ntime(OPTION[u32]) + version(4) + merkle_root(U256, 32)
//
// CRITICAL: this is the Standard Channel job format — fundamentally
// different from Extended Channel jobs (NewExtendedMiningJob, msg 0x1F),
// which DO carry raw coinbase_tx_prefix/suffix + merkle_path for the client
// to assemble its own coinbase. Standard Channel clients never see the
// coinbase at all; the SERVER computes the final merkle root (folding the
// coinbase tx hash through the merkle path) and sends only that 32-byte
// result. An earlier version of this function sent the Extended format to
// Standard Channels — confirmed via live Bitaxe Gamma testing, where the
// firmware accepted the message (no parse error, since the first few fixed
// fields happened to still align) but never actually started hashing,
// because everything past channel_id/job_id was structurally wrong.
//
// min_ntime is OPTION[u32]: a 1-byte presence flag (0 or 1), followed by
// 4 bytes ONLY if present. The spec requires the very first NewMiningJob
// sent after a channel opens to have min_ntime UNSET (a "future job",
// activated later by a matching SetNewPrevHash). minNtimeSet=false encodes
// that case.
func EncodeNewMiningJob(
	channelID uint32,
	jobID uint32,
	minNtimeSet bool,
	minNtime uint32,
	version uint32,
	merkleRoot [32]byte,
) []byte {
	optionLen := 1
	if minNtimeSet {
		optionLen += 4
	}

	size := 4 + 4 + optionLen + 4 + 32
	b := make([]byte, size)
	off := 0

	putU32LE(b, off, channelID)
	off += 4
	putU32LE(b, off, jobID)
	off += 4

	if minNtimeSet {
		b[off] = 1
		off++
		putU32LE(b, off, minNtime)
		off += 4
	} else {
		b[off] = 0
		off++
	}

	putU32LE(b, off, version)
	off += 4

	copy(b[off:], merkleRoot[:])

	return b
}

// ──────────────────────────────────────────────────────────────────────────────
// NewExtendedMiningJob (server → client) — EXTENDED CHANNEL format
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.16:
//
//	channel_id(4) + job_id(4) + min_ntime(OPTION[u32]) + version(4) +
//	merkle_path(SEQ0_255[U256]) + coinbase_tx_prefix(B0_64K) +
//	coinbase_tx_suffix(B0_64K)
//
// Unlike Standard Channel jobs, this sends the RAW coinbase halves and
// merkle branch so the client can insert its own extranonce_prefix +
// self-rolled extranonce2 and fold its own merkle root. coinbaseTxPrefix /
// coinbaseTxSuffix here are exactly coinb1 / coinb2 as used elsewhere in
// this package (JobTemplate.Coinbase1/2, or a channel's solo-mode
// override) — do NOT splice extranonce_prefix into either half; the client
// prepends extranonce_prefix (from OpenExtendedMiningChannel.Success)
// itself, immediately followed by its own rolled extranonce2, between
// coinbase_tx_prefix and coinbase_tx_suffix.
//
// *** VERIFY ***: same caveat as EncodeOpenExtendedMiningChannelSuccess —
// field order matches the spec doc, not yet confirmed against a live
// NerdQAxe++ capture.
func EncodeNewExtendedMiningJob(
	channelID uint32,
	jobID uint32,
	minNtimeSet bool,
	minNtime uint32,
	version uint32,
	versionRollingAllowed bool,
	merklePath [][32]byte,
	coinbaseTxPrefix []byte,
	coinbaseTxSuffix []byte,
) ([]byte, error) {
	optionLen := 1
	if minNtimeSet {
		optionLen += 4
	}

	merkleEnc := encodeSeqU256(merklePath)
	prefixEnc, err := encodeB0_64K(coinbaseTxPrefix)
	if err != nil {
		return nil, fmt.Errorf("NewExtendedMiningJob coinbase_tx_prefix: %w", err)
	}
	suffixEnc, err := encodeB0_64K(coinbaseTxSuffix)
	if err != nil {
		return nil, fmt.Errorf("NewExtendedMiningJob coinbase_tx_suffix: %w", err)
	}

	// +1 for version_rolling_allowed (BOOL, 1 byte)
	size := 4 + 4 + optionLen + 4 + 1 + len(merkleEnc) + len(prefixEnc) + len(suffixEnc)
	b := make([]byte, size)
	off := 0

	putU32LE(b, off, channelID)
	off += 4
	putU32LE(b, off, jobID)
	off += 4

	if minNtimeSet {
		b[off] = 1
		off++
		putU32LE(b, off, minNtime)
		off += 4
	} else {
		b[off] = 0
		off++
	}

	putU32LE(b, off, version)
	off += 4

	// version_rolling_allowed: BOOL (1 byte) — per spec §5.3.16,
	// sits between version and merkle_path. We always allow version
	// rolling (NerdQAxe++ and most SV2 devices expect true here).
	if versionRollingAllowed {
		b[off] = 1
	} else {
		b[off] = 0
	}
	off++

	copy(b[off:], merkleEnc)
	off += len(merkleEnc)
	copy(b[off:], prefixEnc)
	off += len(prefixEnc)
	copy(b[off:], suffixEnc)

	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SubmitSharesStandard (client → server)
// ──────────────────────────────────────────────────────────────────────────────

// MsgSubmitSharesStandardFields is the decoded payload.
type MsgSubmitSharesStandardFields struct {
	ChannelID   uint32
	SequenceNum uint32 // monotonically increasing per channel, for tracking acks
	JobID       uint32
	Nonce       uint32
	NTime       uint32
	Version     uint32 // miner may roll version bits within versionRollingAllowedBits
}

// DecodeSubmitSharesStandard parses the payload.
func DecodeSubmitSharesStandard(payload []byte) (*MsgSubmitSharesStandardFields, error) {
	const need = 24
	if len(payload) < need {
		return nil, errShort("SubmitSharesStandard", len(payload), need)
	}
	return &MsgSubmitSharesStandardFields{
		ChannelID:   getU32LE(payload, 0),
		SequenceNum: getU32LE(payload, 4),
		JobID:       getU32LE(payload, 8),
		Nonce:       getU32LE(payload, 12),
		NTime:       getU32LE(payload, 16),
		Version:     getU32LE(payload, 20),
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SubmitSharesExtended (client → server)
//
// Same as SubmitSharesStandard plus the miner's self-rolled extranonce2,
// since extended-channel clients build their own coinbase per share.
// ──────────────────────────────────────────────────────────────────────────────

// MsgSubmitSharesExtendedFields is the decoded payload.
type MsgSubmitSharesExtendedFields struct {
	ChannelID   uint32
	SequenceNum uint32
	JobID       uint32
	Nonce       uint32
	NTime       uint32
	Version     uint32
	Extranonce  []byte // B0_32 — miner-rolled extranonce2, up to the channel's granted extranonce_size
}

// DecodeSubmitSharesExtended parses the payload.
//
// Wire order per sv2-spec 05-Mining-Protocol.md §5.3.20:
//
//	channel_id(4) + sequence_number(4) + job_id(4) + nonce(4) + ntime(4) +
//	version(4) + extranonce(B0_32)
func DecodeSubmitSharesExtended(payload []byte) (*MsgSubmitSharesExtendedFields, error) {
	const fixedNeed = 24
	if len(payload) < fixedNeed+1 {
		return nil, errShort("SubmitSharesExtended", len(payload), fixedNeed+1)
	}
	m := &MsgSubmitSharesExtendedFields{
		ChannelID:   getU32LE(payload, 0),
		SequenceNum: getU32LE(payload, 4),
		JobID:       getU32LE(payload, 8),
		Nonce:       getU32LE(payload, 12),
		NTime:       getU32LE(payload, 16),
		Version:     getU32LE(payload, 20),
	}
	extranonce, _, err := decodeB0_32(payload, fixedNeed)
	if err != nil {
		return nil, fmt.Errorf("SubmitSharesExtended extranonce: %w", err)
	}
	m.Extranonce = extranonce
	return m, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// SubmitSharesSuccess (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSubmitSharesSuccess acknowledges a range of shares up to lastSeqNum.
func EncodeSubmitSharesSuccess(channelID uint32, lastSeqNum uint32, newSubmitsAccepted uint32, newSharesSumDiff uint64) []byte {
	b := make([]byte, 4+4+4+8)
	putU32LE(b, 0, channelID)
	putU32LE(b, 4, lastSeqNum)
	putU32LE(b, 8, newSubmitsAccepted)
	putU64LE(b, 12, newSharesSumDiff)
	return b
}

// ──────────────────────────────────────────────────────────────────────────────
// SubmitSharesError (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSubmitSharesError rejects a share by sequence number.
func EncodeSubmitSharesError(channelID uint32, seqNum uint32, errCode string) ([]byte, error) {
	codeBytes, err := encodeSTR0_255(errCode)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+4+len(codeBytes))
	putU32LE(b, 0, channelID)
	putU32LE(b, 4, seqNum)
	copy(b[8:], codeBytes)
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func errShort(msg string, got, want int) error {
	return fmt.Errorf("sv2 decode %s: payload too short (%d bytes, need ≥ %d)", msg, got, want)
}

// encodeB0_64K encodes a byte slice with a 2-byte little-endian length
// prefix (0-65535 bytes, per sv2-spec's B0_64K type). Used for
// coinbase_tx_prefix / coinbase_tx_suffix in NewExtendedMiningJob, which
// can exceed B0_32's 1-byte length-prefix range.
//
// NOTE: same duplicate-definition caveat as decodeB0_32 above — check
// codec.go first.
func encodeB0_64K(data []byte) ([]byte, error) {
	if len(data) > 65535 {
		return nil, fmt.Errorf("sv2 encode B0_64K: data too long (%d bytes, max 65535)", len(data))
	}
	b := make([]byte, 2+len(data))
	putU16LE(b, 0, uint16(len(data)))
	copy(b[2:], data)
	return b, nil
}

// encodeSeqU256 encodes a sequence of 32-byte hashes with a 1-byte count
// prefix (sv2-spec's SEQ0_255[U256] type, used for NewExtendedMiningJob's
// merkle_path). Caps at 255 elements — ample for any real merkle branch.
func encodeSeqU256(hashes [][32]byte) []byte {
	if len(hashes) > 255 {
		hashes = hashes[:255] // should never happen in practice; defensive cap
	}
	b := make([]byte, 1+32*len(hashes))
	b[0] = uint8(len(hashes))
	for i, h := range hashes {
		copy(b[1+i*32:], h[:])
	}
	return b
}
