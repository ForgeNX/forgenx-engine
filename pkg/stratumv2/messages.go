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
	MsgNewMiningJob                     uint8 = 0x1A
	MsgSetNewPrevHash                   uint8 = 0x20
	MsgSubmitSharesStandard             uint8 = 0x1E
	MsgSubmitSharesSuccess              uint8 = 0x1F
	MsgSubmitSharesError                uint8 = 0x21
	MsgSetTarget                        uint8 = 0x22
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
	MaxTargetBytes  []byte  // B0_32 — maximum acceptable target (256-bit, LE)
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

	m.MaxTargetBytes, _, err = decodeB0_32(payload, offset)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenStandardMiningChannel.Success (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeOpenStandardMiningChannelSuccess builds the success response.
func EncodeOpenStandardMiningChannelSuccess(
	requestID uint32,
	channelID uint32,
	targetBytes []byte,
	extranonce2size uint16,
	groupChannelID uint32,
) ([]byte, error) {
	targetEnc, err := encodeB0_32(targetBytes)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4+4+len(targetEnc)+2+4)
	putU32LE(b, 0, requestID)
	putU32LE(b, 4, channelID)
	copy(b[8:], targetEnc)
	off := 8 + len(targetEnc)
	putU16LE(b, off, extranonce2size)
	putU32LE(b, off+2, groupChannelID)
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
// SetTarget (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeSetTarget builds the payload for a SetTarget message.
func EncodeSetTarget(channelID uint32, targetBytes []byte) ([]byte, error) {
	targetEnc, err := encodeB0_32(targetBytes)
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
// NewMiningJob (server → client)
// ──────────────────────────────────────────────────────────────────────────────

// EncodeNewMiningJob builds the payload for a NewMiningJob message.
func EncodeNewMiningJob(
	channelID uint32,
	jobID uint32,
	futureJob bool,
	version uint32,
	versionRollingAllowedBits uint32,
	merkleBranch [][32]byte,
	coinbase1 []byte,
	coinbase2 []byte,
) []byte {
	futureJobByte := byte(0)
	if futureJob {
		futureJobByte = 1
	}

	branchLen := len(merkleBranch)
	cb1Len := len(coinbase1)
	cb2Len := len(coinbase2)

	size := 4 + 4 + 1 + 4 + 4 + 1 + branchLen*32 + 2 + cb1Len + 2 + cb2Len
	b := make([]byte, size)
	off := 0

	putU32LE(b, off, channelID)
	off += 4
	putU32LE(b, off, jobID)
	off += 4
	b[off] = futureJobByte
	off++
	putU32LE(b, off, version)
	off += 4
	putU32LE(b, off, versionRollingAllowedBits)
	off += 4

	b[off] = byte(branchLen)
	off++
	for _, h := range merkleBranch {
		copy(b[off:], h[:])
		off += 32
	}

	putU16LE(b, off, uint16(cb1Len))
	off += 2
	copy(b[off:], coinbase1)
	off += cb1Len

	putU16LE(b, off, uint16(cb2Len))
	off += 2
	copy(b[off:], coinbase2)

	return b
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
