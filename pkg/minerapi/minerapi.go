// Package minerapi reads a miner's own view of itself — hashrate, model, the
// pool username it is mining under — over the miner's management API.
//
// A miner's reported hashrate is exact from its first second at full speed,
// where anything measured at the pool has to be inferred from shares and takes
// minutes to settle. Two API families cover nearly every miner sold:
//
//   - AxeOS HTTP (/api/system/info): Bitaxe, NerdQAxe and most open-source
//     ESP32 miners.
//   - CGMiner TCP (port 4028, {"command":...}): stock Antminer and Whatsminer
//     firmware, Avalon, and the aftermarket firmwares — Braiins OS, LuxOS,
//     Vnish — at least for reads.
//
// Field names and units differ between vendors, so every figure is normalised
// here to hashes per second.
package minerapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Reading is one answer from a miner.
type Reading struct {
	Driver     string  // which API answered
	Hashrate   float64 // H/s, the most recent figure the miner gives
	Hashrate10 float64 // H/s, a longer average where the miner offers one; else Hashrate
	Model      string  // the product, e.g. "NerdQAxe++", "Bitaxe Gamma", "Avalon Nano3s"
	Chip       string  // the ASIC, where the miner reports it, e.g. "BM1370"
	PoolUser   string  // the username the miner authorizes with, for matching to a worker
	Hostname   string
}

// Driver is one miner API family.
type Driver interface {
	Name() string
	// Hints reports whether a stratum subscribe string ("bitaxe/BM1370/v2.15.1",
	// "bmminer/2.0.0") suggests this family, so it can be tried first.
	Hints(subscribe string) bool
	Read(ctx context.Context, host string) (Reading, error)
}

// Drivers in the order they are tried when nothing hints at the family.
var Drivers = []Driver{AxeOS{}, CGMiner{}}

// Probe asks a miner at host for a reading, trying the hinted family first and
// then the rest. It returns the first driver that answers.
func Probe(ctx context.Context, host, subscribe string) (Reading, error) {
	ordered := make([]Driver, 0, len(Drivers))
	for _, d := range Drivers {
		if subscribe != "" && d.Hints(subscribe) {
			ordered = append(ordered, d)
		}
	}
	for _, d := range Drivers {
		if subscribe == "" || !d.Hints(subscribe) {
			ordered = append(ordered, d)
		}
	}
	// Report every driver's failure, not just the last one tried: a miner that
	// speaks one API reports "connection refused" on the others, which would
	// otherwise hide why its own API failed.
	var errs []string
	for _, d := range ordered {
		// Each driver gets its own allowance. A small miner's web server can be
		// slow while it is also mining and serving its own dashboard, and with
		// one shared budget a slow first answer used up the time for the rest.
		dctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
		r, err := d.Read(dctx, host)
		cancel()
		if err == nil {
			return r, nil
		}
		errs = append(errs, d.Name()+": "+err.Error())
	}
	return Reading{}, fmt.Errorf("%s", strings.Join(errs, "; "))
}

// ── AxeOS ────────────────────────────────────────────────────────────────────

type AxeOS struct{}

func (AxeOS) Name() string { return "axeos" }

func (AxeOS) Hints(sub string) bool {
	s := strings.ToLower(sub)
	return strings.Contains(s, "bitaxe") || strings.Contains(s, "nerd") || strings.Contains(s, "axe")
}

func (AxeOS) Read(ctx context.Context, host string) (Reading, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/api/system/info", nil)
	if err != nil {
		return Reading{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Reading{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Reading{}, fmt.Errorf("axeos: HTTP %d", resp.StatusCode)
	}
	var info struct {
		HashRate    float64 `json:"hashRate"`     // GH/s, instantaneous
		HashRate1m  float64 `json:"hashRate_1m"`  // GH/s, newer firmware only
		HashRate10m float64 `json:"hashRate_10m"` // GH/s, newer firmware only
		ASICModel   string  `json:"ASICModel"`
		ASICCount   int     `json:"asicCount"`
		DeviceModel string  `json:"deviceModel"` // NerdQAxe and newer AxeOS builds
		StratumUser string  `json:"stratumUser"`
		Hostname    string  `json:"hostname"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return Reading{}, fmt.Errorf("axeos: %w", err)
	}
	if info.HashRate <= 0 && info.HashRate10m <= 0 {
		return Reading{}, fmt.Errorf("axeos: no hashrate in response")
	}
	live := info.HashRate
	if info.HashRate1m > 0 {
		live = info.HashRate1m // steadier than the instantaneous figure
	}
	r := Reading{
		Driver:   "axeos",
		Hashrate: live * 1e9,
		Model:    axeosProduct(info.DeviceModel, info.ASICModel, info.ASICCount),
		Chip:     info.ASICModel,
		PoolUser: info.StratumUser,
		Hostname: info.Hostname,
	}
	r.Hashrate10 = r.Hashrate
	if info.HashRate10m > 0 {
		r.Hashrate10 = info.HashRate10m * 1e9
	}
	return r, nil
}

// axeosProduct names the device. Firmwares that report a device model are
// taken at their word; otherwise a Bitaxe is named from its chip, since each
// Bitaxe generation used a different one. Best-effort — an unrecognised chip
// is shown as the chip itself rather than guessed at.
func axeosProduct(device, chip string, count int) string {
	if device != "" {
		return device
	}
	names := map[string]string{
		"BM1397": "Bitaxe Max",
		"BM1366": "Bitaxe Ultra",
		"BM1368": "Bitaxe Supra",
		"BM1370": "Bitaxe Gamma",
	}
	name, ok := names[strings.ToUpper(chip)]
	if !ok {
		return chip
	}
	if count > 1 {
		name = fmt.Sprintf("%s (%d chips)", name, count)
	}
	return name
}

// ── CGMiner API ──────────────────────────────────────────────────────────────

type CGMiner struct{}

func (CGMiner) Name() string { return "cgminer" }

func (CGMiner) Hints(sub string) bool {
	s := strings.ToLower(sub)
	for _, k := range []string{"cgminer", "bmminer", "bosminer", "braiins", "luxminer", "btminer", "whatsminer", "avalon", "antminer"} {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

// command sends one CGMiner API command and returns the decoded reply. Several
// firmwares end the reply with a NUL byte, which is not valid JSON.
func cgCommand(ctx context.Context, host, cmd string) (map[string]interface{}, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, "4028"))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		conn.SetDeadline(dl)
	}
	if _, err := fmt.Fprintf(conn, `{"command":"%s"}`, cmd); err != nil {
		return nil, err
	}
	raw, _ := bufio.NewReader(conn).ReadBytes(0)
	raw = []byte(strings.TrimRight(string(raw), "\x00\r\n "))
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("cgminer %s: %w", cmd, err)
	}
	return out, nil
}

// number reads a field that some firmwares send as a number and others as a
// string.
func number(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	}
	return 0
}

func (CGMiner) Read(ctx context.Context, host string) (Reading, error) {
	sum, err := cgCommand(ctx, host, "summary")
	if err != nil {
		return Reading{}, err
	}
	list, _ := sum["SUMMARY"].([]interface{})
	if len(list) == 0 {
		return Reading{}, fmt.Errorf("cgminer: no SUMMARY in reply")
	}
	s, _ := list[0].(map[string]interface{})

	// Vendors disagree on units and on which average they expose. Take the
	// most recent figure available, and the longest average for Hashrate10.
	scale := map[string]float64{"THS": 1e12, "GHS": 1e9, "MHS": 1e6, "KHS": 1e3}
	pick := func(suffixes ...string) float64 {
		for _, suf := range suffixes {
			for unit, mul := range scale {
				if v := number(s[unit+" "+suf]); v > 0 {
					return v * mul
				}
			}
		}
		return 0
	}
	r := Reading{Driver: "cgminer"}
	// A 5-second figure on a small miner swings wildly — an Avalon Nano 3S rated
	// at 6.7 TH/s read 13.4 over 5s against 6.7 over 15m. Prefer a minute for the
	// live number and the longest window for the steady one; fall back to 5s
	// only where the firmware offers nothing longer, as stock Antminer does.
	r.Hashrate = pick("1m", "5m", "15m", "av", "5s")
	r.Hashrate10 = pick("15m", "30m", "5m", "av", "1m", "5s")
	if r.Hashrate <= 0 {
		return Reading{}, fmt.Errorf("cgminer: no hashrate in SUMMARY")
	}
	if r.Hashrate10 <= 0 {
		r.Hashrate10 = r.Hashrate
	}

	// Best-effort extras: the pool username for matching, and the model.
	if pools, err := cgCommand(ctx, host, "pools"); err == nil {
		if pl, _ := pools["POOLS"].([]interface{}); len(pl) > 0 {
			for _, p := range pl {
				pm, _ := p.(map[string]interface{})
				if st, _ := pm["Stratum Active"].(bool); st || len(pl) == 1 {
					r.PoolUser, _ = pm["User"].(string)
					break
				}
			}
		}
	}
	if ver, err := cgCommand(ctx, host, "version"); err == nil {
		if vl, _ := ver["VERSION"].([]interface{}); len(vl) > 0 {
			vm, _ := vl[0].(map[string]interface{})
			if t, _ := vm["Type"].(string); t != "" {
				r.Model = t
			} else if t, _ := vm["PROD"].(string); t != "" {
				r.Model = t
			}
		}
	}
	return r, nil
}

// DefaultTimeout bounds a single probe, so an unreachable miner costs seconds
// rather than stalling whatever is asking.
const DefaultTimeout = 5 * time.Second
