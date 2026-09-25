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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Reading is one answer from a miner.
type Reading struct {
	Driver       string  // which API answered
	Host         string  // the address it answered on
	Hashrate     float64 // H/s, the most recent figure the miner gives
	Hashrate10   float64 // H/s, a longer average where the miner offers one; else Hashrate
	Model        string  // the product, e.g. "NerdQAxe++", "Bitaxe Gamma", "Avalon Nano3s"
	Chip         string  // the ASIC, where the miner reports it, e.g. "BM1370"
	PoolUser     string  // the username the miner authorizes with, for matching to a worker
	PoolURL      string  // the pool it is mining to, host:port
	PoolProtocol string  // the protocol set for that pool, where the miner reports it
	Hostname     string
	ASICTemp     float64 // °C, the hottest ASIC reading the miner offers; 0 when unknown
	ASICTempMax  float64 // °C, the hottest single chip, where the miner reports it separately from ASICTemp
	VRTemp       float64 // °C, voltage regulator; 0 when the miner has no such sensor
}

// sensor treats the values firmwares use for "no sensor fitted" — zero, -1,
// the Avalon's -273 — as unknown rather than a reading.
func sensor(v float64) float64 {
	if v <= 0 || v < -100 {
		return 0
	}
	return v
}

// firstSensor returns the first value that is a real reading.
func firstSensor(vals ...float64) float64 {
	for _, v := range vals {
		if v = sensor(v); v > 0 {
			return v
		}
	}
	return 0
}

func hottest(vals ...float64) float64 {
	var m float64
	for _, v := range vals {
		if v = sensor(v); v > m {
			m = v
		}
	}
	return m
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
			r.Model = displayModel(r.Model)
			return r, nil
		}
		errs = append(errs, d.Name()+": "+err.Error())
	}
	return Reading{}, fmt.Errorf("%s", strings.Join(errs, "; "))
}

// displayModels tidies model names firmwares report awkwardly, so every screen
// shows the product's proper name. Anything not listed is shown as reported.
var displayModels = map[string]string{
	"avalon nano3s": "Avalon Nano 3S",
	"avalon nano3":  "Avalon Nano 3",
}

func displayModel(raw string) string {
	if pretty, ok := displayModels[strings.ToLower(strings.TrimSpace(raw))]; ok {
		return pretty
	}
	return raw
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
		HashRate    float64   `json:"hashRate"`     // GH/s, instantaneous
		HashRate1m  float64   `json:"hashRate_1m"`  // GH/s, newer firmware only
		HashRate10m float64   `json:"hashRate_10m"` // GH/s, newer firmware only
		ASICModel   string    `json:"ASICModel"`
		ASICCount   int       `json:"asicCount"`
		DeviceModel string    `json:"deviceModel"` // NerdQAxe and newer AxeOS builds
		Temp        float64   `json:"temp"`
		Temp2       float64   `json:"temp2"`
		ASICTemps   []float64 `json:"asicTemps"` // per chip; NerdQAxe reports zeros
		VRTemp      float64   `json:"vrTemp"`
		VRTempInt   float64   `json:"vrTempInt"` // NerdQAxe's second regulator reading
		StratumUser string    `json:"stratumUser"`
		StratumURL  string    `json:"stratumURL"`
		StratumPort int       `json:"stratumPort"`
		// Some firmwares report this as a name ("SV2"), others as a number, so it
		// is read loosely and normalised below.
		StratumProtocol json.RawMessage `json:"stratumProtocol"`
		Hostname        string          `json:"hostname"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return Reading{}, fmt.Errorf("axeos: %w", err)
	}
	// A miner reporting zero is still answering: an AxeOS miner that has just
	// reconnected to its pool restarts its counters and reads zero for a short
	// while. Its model, temperatures and pool user are still worth having, and
	// whatever uses the reading falls back to measured hashrate until it recovers.
	// Only a response that isn't AxeOS at all is treated as a failure.
	if info.ASICModel == "" && info.Hostname == "" {
		return Reading{}, fmt.Errorf("axeos: response does not look like AxeOS")
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
		// Report what the miner's own dashboard shows — temp for the ASIC, vrTemp
		// for the regulator — so the two never disagree. NerdQAxe also reports
		// vrTempInt, the regulator chip's internal reading, which runs several
		// degrees hotter; it is only used when vrTemp is absent.
		ASICTemp:     firstSensor(info.Temp, hottest(info.ASICTemps...), info.Temp2),
		VRTemp:       firstSensor(info.VRTemp, info.VRTempInt),
		PoolUser:     info.StratumUser,
		PoolURL:      axeosPool(info.StratumURL, info.StratumPort),
		PoolProtocol: protocolName(info.StratumProtocol),
		Hostname:     info.Hostname,
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

// protocolName normalises however a firmware reports its stratum protocol: a
// name on some, a number on others, where 0 is V1 and 1 is V2. Reading it
// strictly cost four miners from the scanner's list, since a firmware that
// answers differently broke the whole reading.
func protocolName(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		switch n {
		case 0:
			return "SV1"
		case 1:
			return "SV2"
		}
	}
	return strings.Trim(string(raw), `"`)
}

// axeosPool renders AxeOS's separate host and port as one pool address.
func axeosPool(host string, port int) string {
	if host == "" {
		return ""
	}
	if port > 0 {
		return fmt.Sprintf("%s:%d", host, port)
	}
	return host
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
					r.PoolURL, _ = pm["URL"].(string)
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
	// Braiins and other firmwares leave the model out of version and put it on
	// each hash board instead, so a 50 TH/s S19 would otherwise show as a blank
	// device. Every board reports the same machine, so the first will do.
	if r.Model == "" {
		if dd, err := cgCommand(ctx, host, "devdetails"); err == nil {
			list, _ := dd["DEVDETAILS"].([]interface{})
			for _, d := range list {
				dm, _ := d.(map[string]interface{})
				if t, _ := dm["Model"].(string); t != "" {
					r.Model = t
					break
				}
			}
		}
	}
	x := cgExtended(ctx, host)
	r.ASICTemp, r.ASICTempMax = x.avg, x.max
	// An Avalon's own current speed is steady where the summary's 5-second
	// figure is not: 6.61 TH/s against 13.4 at the same moment. Its GHSavg is
	// an average since boot — sixteen days on one test unit — so the steady
	// figure stays with the summary's 15-minute window.
	if x.ghsSpd > 0 {
		r.Hashrate = x.ghsSpd * 1e9
	}
	return r, nil
}

// bracketed matches the Key[value] pairs Avalon packs into one long string.
var bracketed = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\[([^\]]*)\]`)

// cgExtras is what the extended stats add to a CGMiner summary.
type cgExtras struct {
	avg, max float64 // ASIC temperatures, °C
	ghsSpd   float64 // the miner's own current speed, GH/s, where it reports one
}

// cgExtended reads temperatures and, on Avalons, the current speed. Avalons pack
// these into a Key[value] string in estats: TAvg and TMax across the whole
// machine (MTavg/MTmax are per hash board, and become lists on multi-board
// models), and GHSspd for current speed. Antminers report temp_chip style
// fields in stats instead — that path is untested on real hardware.
func cgExtended(ctx context.Context, host string) cgExtras {
	var x cgExtras
	if est, err := cgCommand(ctx, host, "estats"); err == nil {
		list, _ := est["STATS"].([]interface{})
		for _, e := range list {
			em, _ := e.(map[string]interface{})
			for _, v := range em {
				str, ok := v.(string)
				if !ok || !strings.Contains(str, "TMax[") {
					continue
				}
				f := map[string]string{}
				for _, m := range bracketed.FindAllStringSubmatch(str, -1) {
					f[m[1]] = m[2]
				}
				x.avg = sensor(number(f["TAvg"]))
				x.max = sensor(number(f["TMax"]))
				x.ghsSpd = number(f["GHSspd"])
				if x.avg == 0 {
					x.avg = x.max
				}
				return x
			}
		}
	}
	if st, err := cgCommand(ctx, host, "stats"); err == nil {
		list, _ := st["STATS"].([]interface{})
		var temps []float64
		for _, e := range list {
			em, _ := e.(map[string]interface{})
			for k, v := range em {
				lk := strings.ToLower(k)
				if !strings.HasPrefix(lk, "temp_chip") && !strings.HasPrefix(lk, "temp2_") {
					continue
				}
				if str, ok := v.(string); ok {
					for _, part := range strings.Split(str, "-") {
						temps = append(temps, number(part))
					}
				} else {
					temps = append(temps, number(v))
				}
			}
		}
		x.max = hottest(temps...)
		x.avg = x.max
	}
	return x
}

// DefaultTimeout bounds a single probe, so an unreachable miner costs seconds
// rather than stalling whatever is asking.
const DefaultTimeout = 5 * time.Second

// SetPool points a miner at a new primary pool. Only AxeOS accepts this: an
// Avalon's firmware has no command to add or change a pool, only to reorder the
// ones it already has.
//
// It changes the pool and worker name and nothing else - the fallback pools,
// frequency, voltage and the rest are left exactly as the owner set them - then
// restarts the miner, since AxeOS applies a pool change on restart.
func SetPool(ctx context.Context, host, poolHost string, poolPort int, worker string) error {
	// Make sure it is AxeOS before writing anything to it.
	r, err := (AxeOS{}).Read(ctx, host)
	if err != nil {
		return fmt.Errorf("%s does not answer as an AxeOS miner: %w", host, err)
	}
	// The mesh speaks V1, so the protocol goes with the address: a miner left on
	// SV2 cannot reach it and quietly falls back to its other pool. Extranonce
	// subscription lets the mesh move it between coins without a reconnect.
	body, err := json.Marshal(map[string]interface{}{
		"stratumURL":                 poolHost,
		"stratumPort":                poolPort,
		"stratumUser":                worker,
		"stratumProtocol":            "SV1",
		"stratumExtranonceSubscribe": true,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, "http://"+host+"/api/system", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("setting the pool on %s: %w", host, err)
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("setting the pool on %s: HTTP %d", host, resp.StatusCode)
	}

	// Confirm it took, rather than assuming.
	after, err := (AxeOS{}).Read(ctx, host)
	if err != nil {
		return fmt.Errorf("%s did not answer after the change: %w", host, err)
	}
	want := axeosPool(poolHost, poolPort)
	if after.PoolURL != want || after.PoolUser != worker {
		return fmt.Errorf("%s did not take the change: pool is %q as %q", host, after.PoolURL, after.PoolUser)
	}
	if !strings.EqualFold(after.PoolProtocol, "SV1") {
		return fmt.Errorf("%s kept protocol %q on its pool, which the mesh does not speak", host, after.PoolProtocol)
	}
	_ = r

	// AxeOS applies a pool change on restart.
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, "http://"+host+"/api/system/restart", nil)
	if err != nil {
		return err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		// The change is saved; it simply has not restarted. Worth saying so
		// rather than reporting the whole thing as a failure.
		return fmt.Errorf("%s saved the pool but would not restart: %w", host, err)
	}
	resp.Body.Close()
	return nil
}
