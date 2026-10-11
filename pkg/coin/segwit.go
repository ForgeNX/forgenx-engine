package coin

import (
	"fmt"
	"strings"
)

// SegWit addresses of every witness version, as BIP350 has them: version 0
// (bc1q...) in Bech32, version 1 and later (taproot, bc1p...) in Bech32m.
//
// DecodeBech32Address (bech32.go) only takes Bech32, so taproot addresses
// fail its checksum; the coins that use it are unchanged by this file. A coin
// that can pay to taproot uses DecodeSegwitAddress and SegwitScript instead.

const bech32mConst = 0x2bc830a3

// DecodeSegwitAddress decodes a SegWit address of any witness version (0-16),
// checking it's in the encoding its version needs, and returns the witness
// version and program.
func DecodeSegwitAddress(address, expectedHRP string) (byte, []byte, error) {
	if len(address) > 90 {
		return 0, nil, fmt.Errorf("segwit address too long")
	}
	lower := strings.ToLower(address)
	if address != lower && address != strings.ToUpper(address) {
		return 0, nil, fmt.Errorf("mixed case in segwit address")
	}
	pos := strings.LastIndex(lower, "1")
	if pos < 1 || pos+7 > len(lower) {
		return 0, nil, fmt.Errorf("invalid segwit address separator position")
	}
	hrp, dataStr := lower[:pos], lower[pos+1:]
	if hrp != expectedHRP {
		return 0, nil, fmt.Errorf("unexpected HRP: got %s, want %s", hrp, expectedHRP)
	}
	data := make([]int, len(dataStr))
	for i, c := range dataStr {
		if c > 127 || bech32CharsetRev[c] == -1 {
			return 0, nil, fmt.Errorf("invalid bech32 character: %c", c)
		}
		data[i] = int(bech32CharsetRev[c])
	}
	check := bech32Polymod(append(bech32HRPExpand(hrp), data...))
	data = data[:len(data)-6]
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("empty segwit address data")
	}

	version := data[0]
	if version > 16 {
		return 0, nil, fmt.Errorf("invalid witness version %d", version)
	}
	// Version 0 must be Bech32 and later versions Bech32m (BIP350).
	if version == 0 && check != 1 {
		return 0, nil, fmt.Errorf("invalid checksum for a version 0 segwit address")
	}
	if version != 0 && check != bech32mConst {
		return 0, nil, fmt.Errorf("invalid checksum for a version %d segwit address", version)
	}

	program, err := ConvertBits(data[1:], 5, 8, false)
	if err != nil {
		return 0, nil, fmt.Errorf("converting bits: %w", err)
	}
	if len(program) < 2 || len(program) > 40 {
		return 0, nil, fmt.Errorf("invalid witness program length: %d", len(program))
	}
	if version == 0 && len(program) != 20 && len(program) != 32 {
		return 0, nil, fmt.Errorf("invalid witness program length for v0: %d", len(program))
	}
	out := make([]byte, len(program))
	for i, v := range program {
		out[i] = byte(v)
	}
	return byte(version), out, nil
}

// SegwitScript is the output script paying to a witness program: OP_n
// followed by the program (P2WPKH and P2WSH for version 0, P2TR for a
// version 1 32-byte program).
func SegwitScript(version byte, program []byte) ([]byte, error) {
	switch {
	case version == 0 && (len(program) == 20 || len(program) == 32):
		return append([]byte{0x00, byte(len(program))}, program...), nil
	case version == 1 && len(program) == 32:
		return append([]byte{0x51, 0x20}, program...), nil
	}
	return nil, fmt.Errorf("unsupported witness program (version %d, %d bytes)", version, len(program))
}
