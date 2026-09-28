package coinapi

import (
	"strings"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/minerapi"
)

// Each node's highest total hashrate since the engine started, worked out from
// the same figures the Nexus Miners and Nodes tabs show: a miner's own reading
// where the LAN scanner has one, the relay's measurement for a meshed miner the
// scanner cannot read, and otherwise the node's 15-minute average for it. Each
// miner counts once, on the node it is mining; the mesh's warm standby sessions
// on other nodes are left out.
//
// Kept for both of the miners' own figures, their most recent ("live") and
// their longer average, so the peak matches whichever the Nodes tab shows. The
// pool figure in max_hashrate, which the coin apps use, is estimated from
// submitted shares and can spike far above what the miners are doing.

const nodePeakEvery = 30 * time.Second // the scanner's own polling pace

// runNodePeaks samples every node's total on a steady tick for the life of the
// process.
func (c *CoinAPI) runNodePeaks() {
	time.Sleep(15 * time.Second) // let the scanner take its first readings
	ticker := time.NewTicker(nodePeakEvery)
	defer ticker.Stop()
	for {
		c.sampleNodePeaks()
		<-ticker.C
	}
}

// sampleNodePeaks totals each node's hashrate now and raises its peaks.
func (c *CoinAPI) sampleNodePeaks() {
	minersData, err := c.fetchEngineJSON("/miners")
	if err != nil {
		return
	}
	miners, _ := minersData["miners"].(map[string]interface{})

	active := map[string]string{}
	if c.meshActiveCoins != nil {
		active = c.meshActiveCoins()
	}
	measured := map[string][2]float64{}
	if c.meshHashrates != nil {
		measured = c.meshHashrates()
	}
	readings := map[string]minerapi.Reading{}
	if c.scanner != nil {
		readings = c.scanner.Readings()
	}

	live := map[string]float64{}
	avg := map[string]float64{}
	counted := map[string]bool{}
	for symbol, raw := range miners {
		sym := strings.ToUpper(symbol)
		list, _ := raw.([]interface{})
		for _, mRaw := range list {
			m, ok := mRaw.(map[string]interface{})
			if !ok {
				continue
			}
			name := getString(m, "worker_name")
			if strings.TrimSpace(name) == "" {
				continue
			}
			worker := minerapi.WorkerOf(name)
			coin, meshed := active[worker]
			if meshed && !strings.EqualFold(coin, symbol) {
				continue // a warm standby session, not where it is mining
			}
			if counted[worker] {
				continue
			}
			counted[worker] = true

			var l, a float64 // TH/s
			rd, haveMiner := readings[worker]
			mh, haveMesh := measured[worker]
			switch {
			case haveMiner && rd.Hashrate > 0:
				l = rd.Hashrate / 1e12
				a = rd.Hashrate10 / 1e12
				if a <= 0 {
					a = l
				}
			case meshed && haveMesh && mh[1] >= 5:
				l = mh[0] / 1e12
				a = l
			default:
				a = getFloat(m, "hashrate_15m")
				l = a
			}
			live[sym] += l
			avg[sym] += a
		}
	}

	c.nodePeakMu.Lock()
	defer c.nodePeakMu.Unlock()
	c.nodeNowLive, c.nodeNowAvg = live, avg
	if c.nodePeakLive == nil {
		c.nodePeakLive = map[string]float64{}
		c.nodePeakAvg = map[string]float64{}
	}
	for sym, v := range live {
		if v > c.nodePeakLive[sym] {
			c.nodePeakLive[sym] = v
		}
	}
	for sym, v := range avg {
		if v > c.nodePeakAvg[sym] {
			c.nodePeakAvg[sym] = v
		}
	}
}

// nodeNow returns a node's latest live and average totals, TH/s, as the
// history charts record them.
func (c *CoinAPI) nodeNow(symbol string) (live, avg float64) {
	c.nodePeakMu.Lock()
	defer c.nodePeakMu.Unlock()
	sym := strings.ToUpper(symbol)
	return c.nodeNowLive[sym], c.nodeNowAvg[sym]
}

// nodePeaks returns a node's peak live and average totals this session, TH/s.
func (c *CoinAPI) nodePeaks(symbol string) (live, avg float64) {
	c.nodePeakMu.Lock()
	defer c.nodePeakMu.Unlock()
	sym := strings.ToUpper(symbol)
	return c.nodePeakLive[sym], c.nodePeakAvg[sym]
}
