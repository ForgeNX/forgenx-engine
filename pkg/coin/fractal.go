package coin

import (
	"encoding/hex"
	"fmt"

	"github.com/ForgeNX/forgenx-engine/pkg/coinbase"
	"github.com/ForgeNX/forgenx-engine/pkg/noderpc"
)

// Fractal implements the Coin interface for Fractal Bitcoin (FB).
//
// Fractal's node is Bitcoin Core's code with its own chain, so its block
// template, coinbase and blocks are Bitcoin's (SegWit, witness commitment),
// and so are its addresses (bc1q..., 1..., 3...). Most Fractal wallets give
// taproot addresses (bc1p...), so those are taken as payout addresses too.
// (Blocks found here are Fractal's directly-mined ones; its merged-mined
// blocks are a separate track, not built here.)
type Fractal struct{}

func init() {
	Register("fractal", &Fractal{})
}

func (f *Fractal) Name() string         { return "Fractal Bitcoin" }
func (f *Fractal) Symbol() string       { return "FB" }
func (f *Fractal) Algorithm() string    { return "sha256d" }
func (f *Fractal) SupportsSegWit() bool { return true }

func (f *Fractal) Params() CoinParams {
	return CoinParams{
		P2PKHVersionMainnet: 0x00,
		P2PKHVersionTestnet: 0x6F,
		P2SHVersionMainnet:  0x05,
		P2SHVersionTestnet:  0xC4,
		Bech32HRPMainnet:    "bc",
		Bech32HRPTestnet:    "tb",
		DefaultRPCPort:      8332,
		SegWit:              true,
	}
}

func (f *Fractal) TemplateRules() []string {
	return []string{"segwit"}
}

// addressParams: the HRP and Base58 versions for a network (regtest has its
// own HRP; its Base58 versions are testnet's).
func (f *Fractal) addressParams(network string) (hrp string, p2pkh, p2sh byte) {
	p := f.Params()
	switch network {
	case "testnet", "testnet4", "signet":
		return p.Bech32HRPTestnet, p.P2PKHVersionTestnet, p.P2SHVersionTestnet
	case "regtest":
		return "bcrt", p.P2PKHVersionTestnet, p.P2SHVersionTestnet
	}
	return p.Bech32HRPMainnet, p.P2PKHVersionMainnet, p.P2SHVersionMainnet
}

func (f *Fractal) ValidateAddress(address, network string) error {
	_, err := f.AddressToScript(address, network)
	if err != nil {
		return fmt.Errorf("invalid Fractal Bitcoin address: %s", address)
	}
	return nil
}

func (f *Fractal) AddressToScript(address, network string) ([]byte, error) {
	hrp, p2pkhVersion, p2shVersion := f.addressParams(network)

	// SegWit, any version (taproot included)
	if version, program, err := DecodeSegwitAddress(address, hrp); err == nil {
		return SegwitScript(version, program)
	}

	// Base58Check
	version, payload, err := Base58CheckDecode(address)
	if err != nil {
		return nil, fmt.Errorf("cannot decode address: %s", address)
	}
	if len(payload) != 20 {
		return nil, fmt.Errorf("invalid address payload length: %d", len(payload))
	}
	if version == p2pkhVersion {
		return coinbase.P2PKHScript(payload), nil
	}
	if version == p2shVersion {
		return coinbase.P2SHScript(payload), nil
	}
	return nil, fmt.Errorf("unknown address version: 0x%02x", version)
}

func (f *Fractal) BuildCoinbase(template *noderpc.BlockTemplate, address, network, coinbaseText string,
	extraNonce1Size, extraNonce2Size int, extraOutputs []coinbase.CoinbaseOutput) (string, string, error) {

	script, err := f.AddressToScript(address, network)
	if err != nil {
		return "", "", fmt.Errorf("building output script: %w", err)
	}

	poolValue := template.CoinbaseValue
	for _, eo := range extraOutputs {
		poolValue -= eo.Value
	}
	outputs := []coinbase.CoinbaseOutput{
		{Value: poolValue, Script: script},
	}
	outputs = append(outputs, extraOutputs...)

	// The witness commitment (in the coinbase's txid form, for the merkle root)
	if template.DefaultWitnessCommitment != "" {
		commitment, err := hex.DecodeString(template.DefaultWitnessCommitment)
		if err != nil {
			return "", "", fmt.Errorf("decoding witness commitment: %w", err)
		}
		outputs = append(outputs, coinbase.CoinbaseOutput{
			Value:  0,
			Script: commitment,
		})
	}

	coinb1, coinb2 := coinbase.BuildCoinbaseParts(
		template.Height, coinbaseText,
		extraNonce1Size, extraNonce2Size,
		outputs, true,
	)

	return coinb1, coinb2, nil
}

func (f *Fractal) BuildBlock(header []byte, coinbaseTx []byte, template *noderpc.BlockTemplate) (string, error) {
	// A SegWit block's coinbase carries the witness (marker, flag, reserved value).
	if template.DefaultWitnessCommitment != "" {
		coinbaseTx = coinbase.AddWitnessData(coinbaseTx)
	}

	var block []byte
	block = append(block, header...)
	block = append(block, coinbase.SerializeVarInt(uint64(len(template.Transactions)+1))...)
	block = append(block, coinbaseTx...)

	for _, tx := range template.Transactions {
		txData, err := hex.DecodeString(tx.Data)
		if err != nil {
			return "", fmt.Errorf("decoding transaction: %w", err)
		}
		block = append(block, txData...)
	}

	return hex.EncodeToString(block), nil
}

func (f *Fractal) PoolReward(template *noderpc.BlockTemplate) int64 {
	return template.CoinbaseValue
}
