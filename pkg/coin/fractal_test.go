package coin

import (
	"encoding/hex"
	"testing"
)

// The expected scripts are what Fractal's own node (v0.4.0, validateaddress)
// gives for each address.
func TestFractalAddressToScript(t *testing.T) {
	f := &Fractal{}
	good := []struct{ addr, script string }{
		{"BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4", "0014751e76e8199196d454941c45d1b3a323f1433bd6"},                                             // P2WPKH (upper case)
		{"bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3", "00201863143c14c5166804bd19203356da136c985678cd4d27a1b8c6329604903262"}, // P2WSH
		{"bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0", "512079be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"}, // P2TR
		{"1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "76a91477bff20c60e522dfaa3350c39b030a5d004e839a88ac"},                                               // P2PKH
		{"3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "a914b472a266d0bd89c13706a4132ccfb16f7c3b9fcb87"},                                                   // P2SH
		{"bc1quw6px6ghgttu7xvevz27enxmmqaskrdh2ug0v0", "0014e3b413691742d7cf19996095ecccdbd83b0b0db7"},                                             // AUTHORS' FB address
	}
	for _, g := range good {
		if err := f.ValidateAddress(g.addr, "mainnet"); err != nil {
			t.Errorf("ValidateAddress(%s): %v", g.addr, err)
		}
		script, err := f.AddressToScript(g.addr, "mainnet")
		if err != nil {
			t.Errorf("AddressToScript(%s): %v", g.addr, err)
			continue
		}
		if got := hex.EncodeToString(script); got != g.script {
			t.Errorf("AddressToScript(%s) = %s, want %s", g.addr, got, g.script)
		}
	}

	bad := []string{
		"bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqh2y7hd", // taproot in Bech32, not Bech32m
		"tb1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vq47zagq", // testnet address on mainnet
		"bitcoincash:qrms9qaetpa4ssg8mxz7qtvke2cjrcdr9yt8c9pwky",         // another coin's
		"DHGNWdRFhHRRkxshdSEs9gqWfDMpNBE9RF",                             // DigiByte
		"",
	}
	for _, a := range bad {
		if err := f.ValidateAddress(a, "mainnet"); err == nil {
			t.Errorf("ValidateAddress(%q) accepted a bad address", a)
		}
	}
}

// The other coins keep taking Bech32 only: a taproot address is still refused
// for them, as before.
func TestTaprootStillRefusedElsewhere(t *testing.T) {
	const taproot = "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0"
	if _, _, err := DecodeBech32Address(taproot, "bc"); err == nil {
		t.Errorf("DecodeBech32Address took a Bech32m (taproot) address")
	}
	if err := (&Bitcoin{}).ValidateAddress(taproot, "mainnet"); err == nil {
		t.Errorf("Bitcoin took a taproot address (unchanged behaviour expected)")
	}
}
