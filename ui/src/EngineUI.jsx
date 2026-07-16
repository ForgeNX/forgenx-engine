import { useState, useEffect, useCallback, useRef } from "react"
import { Zap, Users, BarChart2, Clock, Box, Cpu, Wifi, WifiOff, RefreshCw, Server, Hash, Globe, Package, Tag, GitBranch, User, ExternalLink, Headphones, Play, Square, RotateCcw, Download, Copy, Check, ChevronDown } from "lucide-react"

const ENG = {
  color:       "#ff00b4",
  colorDim:    "rgba(255,0,180,0.10)",
  colorBorder: "rgba(255,0,180,0.35)",
  colorGlow:   "rgba(255,0,180,0.2)",
}

const CARD = {
  background: "rgba(2,6,17,0.55)",
  border: `1px solid ${ENG.colorBorder}`,
  borderRadius: "12px",
  padding: "14px 16px",
}

// ── Data hook ────────────────────────────────────────────────────────────────
function useEngineData(online, initialStats, initialMiners) {
  const cached = initialStats ?? (() => { try { const s = sessionStorage.getItem("forgenx_engine_stats"); return s ? JSON.parse(s) : null } catch { return null } })()
  const cachedMiners = initialMiners ?? (() => { try { const s = sessionStorage.getItem("forgenx_engine_miners"); return s ? JSON.parse(s) : {} } catch { return {} } })()
  const [stats,   setStats]   = useState(cached)
  const [miners,  setMiners]  = useState(cachedMiners)
  const [loading, setLoading] = useState(!cached)
  const timerRef = useRef(null)

  const fetch_ = useCallback(async () => {
    if (!online) { setLoading(false); return }
    try {
      const [s, m] = await Promise.all([
        fetch("/api/engine/stats").then(r => r.json()),
        fetch("/api/engine/miners").then(r => r.json()),
      ])
      if (s) { setStats(s); try { sessionStorage.setItem("forgenx_engine_stats", JSON.stringify(s)) } catch {} }
      const mm = m.miners ?? {}
      setMiners(mm)
      try { sessionStorage.setItem("forgenx_engine_miners", JSON.stringify(mm)) } catch {}
    } catch (e) {
      console.error("Engine data fetch failed:", e)
    } finally {
      setLoading(false)
    }
  }, [online])

  useEffect(() => {
    // Show cached data immediately, then fetch fresh data
    if (cached) setLoading(false)
    fetch_()
    timerRef.current = setInterval(fetch_, 5000)
    return () => clearInterval(timerRef.current)
  }, [fetch_])

  return { stats, miners, loading }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function formatUptime(sec) {
  if (!sec) return "—"
  const d = Math.floor(sec / 86400)
  const h = Math.floor((sec % 86400) / 3600)
  const m = Math.floor((sec % 3600) / 60)
  if (d > 0) return `${d}d ${h}h`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m`
}

function formatHashrate(h) {
  if (!h || h === 0) return "0 H/s"
  if (h >= 1e18) return `${(h/1e18).toFixed(2)} EH/s`
  if (h >= 1e15) return `${(h/1e15).toFixed(2)} PH/s`
  if (h >= 1e12) return `${(h/1e12).toFixed(2)} TH/s`
  if (h >= 1e9)  return `${(h/1e9).toFixed(2)} GH/s`
  if (h >= 1e6)  return `${(h/1e6).toFixed(2)} MH/s`
  if (h >= 1e3)  return `${(h/1e3).toFixed(2)} KH/s`
  return `${h.toFixed(0)} H/s`
}

function ClickCopy({ text, children, flex, label, value }) {
  const [ok, setOk] = useState(false)
  const copy = () => {
    const finish = () => { setOk(true); setTimeout(() => setOk(false), 2000) }
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(finish).catch(() => fallback())
    } else { fallback() }
    function fallback() {
      const el = document.createElement("textarea")
      el.value = text; el.style.position = "fixed"; el.style.opacity = "0"
      document.body.appendChild(el); el.select()
      try { document.execCommand("copy"); finish() } catch {}
      document.body.removeChild(el)
    }
  }
  return (
    <div onClick={copy} style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "8px", padding: "4px 10px", cursor: "pointer", userSelect: "none", display: "inline-flex", alignItems: "center" }}>
      {label && label}
      {ok ? <div style={{ fontSize: "12px", color: "#22ff88" }}>✓ Copied</div> : (value ?? children)}
    </div>
  )
}

function CopyBtn({ text, color, colorDim, colorBorder }) {
  const [ok, setOk] = useState(false)
  const copy = () => {
    const finish = () => { setOk(true); setTimeout(() => setOk(false), 2000) }
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(finish).catch(() => fallback())
    } else { fallback() }
    function fallback() {
      const el = document.createElement("textarea")
      el.value = text; el.style.position = "fixed"; el.style.opacity = "0"
      document.body.appendChild(el); el.select()
      try { document.execCommand("copy"); finish() } catch {}
      document.body.removeChild(el)
    }
  }
  return (
    <button onClick={copy} style={{ background: ok ? colorDim : "rgba(255,0,180,0.12)", border: `1px solid ${ok ? colorBorder : "rgba(255,0,180,0.4)"}`, color: "#f1f5f9", borderRadius: "6px", padding: "2px 8px", fontSize: "10px", fontWeight: 600, cursor: "pointer", fontFamily: "inherit", flexShrink: 0, alignSelf: "center" }}>
      {ok ? "✓ Copied" : "Copy"}
    </button>
  )
}

function timeAgo(ts) {
  if (!ts) return null
  const diff = Math.floor(Date.now() / 1000) - ts
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff/60)} mins ago`
  if (diff < 86400) return `${Math.floor(diff/3600)} hours ago`
  return `${Math.floor(diff/86400)} days ago`
}

function StatCard({ icon: Icon, label, value, sub, accent }) {
  return (
    <div style={{ ...CARD, flex: "1 1 140px", minWidth: 0, display: "flex", flexDirection: "column", gap: "4px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "4px" }}>
        {Icon && <Icon size={11} color={ENG.color} strokeWidth={1.5} />}
        <span style={{ fontSize: "10px", fontWeight: 600, color: "#cbd5e1", textTransform: "uppercase", letterSpacing: "0.07em" }}>{label}</span>
      </div>
      <div style={{ fontSize: "16px", fontWeight: 700, color: accent ? ENG.color : "#f1f5f9", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: "10px", color: "#94a3b8", marginTop: "2px" }}>{sub}</div>}
    </div>
  )
}

function PoolAddressPill({ port, color, colorBorder }) {
  const [ok, setOk] = useState(false)
  const copy = () => {
    const t = `stratum+tcp://${window.location.hostname}:${port}`
    const finish = () => { setOk(true); setTimeout(() => setOk(false), 2000) }
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(t).then(finish).catch(() => fb(t, finish))
    } else { fb(t, finish) }
    function fb(t, cb) {
      const el = document.createElement("textarea"); el.value = t; el.style.position = "fixed"; el.style.opacity = "0"
      document.body.appendChild(el); el.select()
      try { document.execCommand("copy"); cb() } catch {}
      document.body.removeChild(el)
    }
  }
  return (
    <div onClick={copy} style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${colorBorder}`, borderRadius: "8px", padding: "8px 12px", flex: "1 1 140px", cursor: "pointer", userSelect: "none" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "2px" }}>
        <div style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.07em", color: "#94a3b8" }}>Pool Address</div>
        <div style={{ fontSize: "9px", color: "#cbd5e1" }}>(click to copy)</div>
      </div>
      {ok
        ? <div style={{ fontSize: "12px", color: "#22ff88" }}>✓ Copied</div>
        : <div style={{ fontSize: "12px", fontFamily: "monospace" }}><span style={{ color }}>stratum+tcp://</span><span style={{ color: "#cbd5e1" }}>{window.location.hostname}</span><span style={{ color }}>:{port}</span></div>}
    </div>
  )
}

function CoinCard({ symbol, engineData, nodeData, engineOnline, miners }) {
  const synced = nodeData?.synced ?? false
  const syncPct = nodeData?.sync_pct ?? 0
  const lastBlockTs = nodeData?.last_block_time
  const lastBlockDate = lastBlockTs ? new Date(lastBlockTs * 1000).toLocaleTimeString([], {hour:"2-digit",minute:"2-digit"}).replace(" ", "") + " " + new Date(lastBlockTs * 1000).toLocaleDateString() : null
  const lastBlockAgo = lastBlockTs ? timeAgo(lastBlockTs) : null
  const lastBlock = lastBlockDate ?? "No blocks yet"

  const coinSymbol = symbol?.toUpperCase()
  const coinMiners = Object.values(miners ?? {}).filter(m => (m.coin ?? "").toUpperCase() === coinSymbol).length
  const coinHashrate = Object.values(miners ?? {}).filter(m => (m.coin ?? "").toUpperCase() === coinSymbol).reduce((s, m) => s + ((m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12), 0)
  const totalHashrate = Object.values(miners ?? {}).reduce((s, m) => s + ((m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12), 0)
  const hashratePct = coinHashrate > 0 && totalHashrate > 0 ? (coinHashrate / totalHashrate * 100) : 0

  const row1Stats = [
    ["Sync status",   synced ? `100% Synced` : `${syncPct.toFixed(1)}% Syncing`, synced ? "#22ff88" : "#fbbf24"],
    ["Block height",  nodeData?.blocks != null ? nodeData.blocks.toLocaleString() : "—", "#f1f5f9"],
    ["Last block",    lastBlockDate ? <>{lastBlockDate}{lastBlockAgo && <span style={{fontSize:"10px",color:"#94a3b8",marginLeft:"6px"}}>({lastBlockAgo})</span>}</> : "No blocks yet", "#f1f5f9"],
  ]
  // Pool Address rendered last in row 1
  const row2Stats = [
    ["Worker count",       coinMiners.toLocaleString(), coinMiners > 0 ? "#22ff88" : "#f1f5f9"],
    ["Network difficulty", nodeData?.difficulty ? (nodeData.difficulty / 1e9).toFixed(2) + " G" : "—", "#f1f5f9"],
    ["Network hashrate",   nodeData?.network_hashrate ?? "—", "#f1f5f9"],
    ["Best session diff",  "—", "#f1f5f9"],
    ["Est. time to block", "—", "#f1f5f9"],
  ]

  const nodeOnline = nodeData?.status === "online"
  const zmqOk = nodeData?.zmq_connected ?? false
  const readiness = [
    { label: "Node RPC",          ok: nodeOnline,                                                                        warnColor: "#ff0080" },
    { label: "Blockchain synced", ok: synced,                                                                            warnColor: nodeOnline ? "#f59e0b" : "#ff0080" },
    { label: "Engine online",     ok: engineOnline,                                                                      warnColor: "#ff0080" },
    { label: "Stratum port",      ok: !!(nodeData?.stratum_port),                                                        warnColor: "#ff0080" },
    { label: nodeOnline ? (zmqOk ? "ZMQ connected" : "Polling (no ZMQ)") : "Not connected", ok: nodeOnline && zmqOk,   okColor: "#22c55e", warnColor: nodeOnline ? "#ff0080" : "#ff0080" },
  ]

  return (
    <div style={{ ...CARD, flex: "1 1 320px", minWidth: 0 }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "10px", paddingBottom: "8px", borderBottom: `1px solid ${ENG.colorBorder}` }}>
        <img src={`/nodes/${symbol}.png`} alt={symbol}
          style={{ width: "28px", height: "28px", borderRadius: "6px", objectFit: "contain",
            filter: `drop-shadow(0 0 6px ${ENG.color}88)` }}
          onError={e => { e.target.style.display = "none" }} />
        <span style={{ fontSize: "13px", fontWeight: 700, color: "#f1f5f9" }}>Forge{symbol}</span>
        <div style={{ flex: 1, marginLeft: "16px", display: "flex", alignItems: "center", gap: "8px", background: "rgba(2,6,17,0.7)", borderRadius: "8px", padding: "5px 10px" }}>
          <span style={{ fontSize: "12px", color: "#e2e8f0", flexShrink: 0, fontWeight: 700 }}>{hashratePct.toFixed(0)}%</span>
          <div style={{ flex: 1, height: "8px", background: "rgba(255,255,255,0.06)", borderRadius: "4px", overflow: "hidden" }}>
            <div style={{ height: "100%", width: `${hashratePct}%`, background: "linear-gradient(90deg, #00e5ff, #a855f7, #ff0080)", borderRadius: "4px", transition: "width 0.5s" }} />
          </div>
          <span style={{ fontSize: "12px", color: "#e2e8f0", flexShrink: 0, fontWeight: 700 }}>{formatHashrate(coinHashrate)}</span>
        </div>
      </div>

      {/* Sync progress bar */}
      {!synced && (
        <div style={{ height: "3px", background: "rgba(255,255,255,0.06)", borderRadius: "2px", marginBottom: "10px", overflow: "hidden" }}>
          <div style={{ height: "100%", width: `${syncPct}%`, background: ENG.color, borderRadius: "2px", transition: "width 0.5s" }} />
        </div>
      )}

      <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
        {/* Row 1: Sync Status, Block Height, Last Block, Readiness, Pool Address */}
        {row1Stats.map(([label, val, color]) => (
          <div key={label} style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "8px", padding: "8px 12px", flex: "1 1 140px" }}>
            <div style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.07em", color: "#94a3b8", marginBottom: "2px" }}>{label}</div>
            <div style={{ fontSize: "12px", fontWeight: 600, color }}>{val}</div>
          </div>
        ))}
        <div style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "8px", padding: "8px 12px", flex: "1 1 140px" }}>
          <div style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.07em", color: "#94a3b8", marginBottom: "4px" }}>Readiness</div>
          <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
            {readiness.map(({ label, ok, okColor, warnColor }) => (
              <div key={label} style={{ display: "flex", alignItems: "center", gap: "3px", fontSize: "11px", color: ok ? (okColor ?? "#cbd5e1") : (warnColor ?? "#94a3b8") }}>
                <span style={{ color: ok ? (okColor ?? "#22ff88") : (warnColor ?? "#ff0080"), fontWeight: 700 }}>{ok ? "✓" : "✗"}</span>{label}
              </div>
            ))}
          </div>
        </div>
        {nodeData?.stratum_port
          ? <PoolAddressPill port={nodeData.stratum_port} color={ENG.color} colorBorder={ENG.colorBorder} />
          : <div style={{ flex: "1 1 140px" }} />}
        <div style={{ flexBasis: "100%", height: 0 }} />
        {/* Row 2 pills */}
        {row2Stats.map(([label, val, color]) => (
          <div key={label} style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "8px", padding: "8px 12px", flex: "1 1 140px" }}>
            <div style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.07em", color: "#94a3b8", marginBottom: "2px" }}>{label}</div>
            <div style={{ fontSize: "12px", fontWeight: 600, color }}>{val}</div>
          </div>
        ))}
      </div>
      </div>


  )
}

// ── Dashboard tab ────────────────────────────────────────────────────────────
function EngineDashboard({ stats, miners, loading, engineOnline, nodes }) {
  const minerList = Object.values(miners).flat()
  const totalMiners = minerList.length
  const totalHashrate = minerList.reduce((sum, m) => sum + ((m.hashrate_15m ?? m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12), 0)
  const totalHashrate5m = minerList.reduce((sum, m) => sum + ((m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12), 0)
  const coins = stats?.coins ?? {}
  const totalAccepted = Object.values(coins).reduce((s, c) => s + (c.shares_accepted ?? 0), 0)
  const totalRejected = Object.values(coins).reduce((s, c) => s + (c.shares_rejected ?? 0), 0)
  const totalStale    = Object.values(coins).reduce((s, c) => s + (c.shares_stale    ?? 0), 0)
  const totalShares   = totalAccepted + totalRejected + totalStale
  const validPct = totalShares > 0 ? ((totalAccepted / totalShares) * 100).toFixed(1) : "—"
  const totalBlocks = Object.values(coins).reduce((s, c) => s + (c.blocks_found ?? 0), 0)

  if (!engineOnline && !stats) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", flexDirection: "column", gap: "8px" }}>
      <WifiOff size={24} color="#ff0080" />
      <div style={{ fontSize: "13px", color: "#475569" }}>ForgeNX Engine is offline</div>
    </div>
  )

  if (loading && !stats) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#334155", fontSize: "12px" }}>
      Loading…
    </div>
  )

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px", padding: "10px 0" }}>
      {/* Top stats row */}
      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
        <StatCard icon={Zap}      label="Pool Hashrate" value={totalHashrate > 0 ? <>{formatHashrate(totalHashrate)} <span style={{fontSize:"11px",color:"#94a3b8",fontWeight:400}}>(15min)</span>{totalHashrate5m > 0 && <><span style={{color:"#475569",margin:"0 4px"}}>/</span><span style={{fontWeight:700,color:ENG.color}}>{formatHashrate(totalHashrate5m)}</span> <span style={{fontSize:"11px",color:"#94a3b8",fontWeight:400}}>(5min)</span></>}</> : "—"} accent />
        <StatCard icon={Users}    label="Worker Count" value={totalMiners} />
        <StatCard icon={BarChart2} label="Blocks Found" value={<>{totalBlocks}<span style={{fontSize:"11px",color:"#94a3b8",fontWeight:400,marginLeft:"10px"}}>(session)</span></>} />
        <StatCard icon={Hash}     label="Total Shares" value={<>{totalShares > 0 ? totalShares.toLocaleString() : "—"}<span style={{fontSize:"11px",color:"#94a3b8",fontWeight:400,marginLeft:"10px"}}>(session)</span></>} />
        <div style={{ ...CARD, flex: "1 1 140px", minWidth: 0, display: "flex", flexDirection: "column", gap: "4px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "4px" }}>
            <Server size={11} color={ENG.color} strokeWidth={1.5} />
            <span style={{ fontSize: "10px", fontWeight: 600, color: "#cbd5e1", textTransform: "uppercase", letterSpacing: "0.07em" }}>ForgeNX Engine</span>
          </div>
          <div style={{ fontSize: "16px", fontWeight: 700, color: engineOnline ? "#22ff88" : "#ff0080", lineHeight: 1 }}>{engineOnline ? "Online" : "Offline"}</div>

        </div>
        <StatCard icon={Clock}    label="ForgeNX Uptime"  value={formatUptime(stats?.uptime_seconds)} />
      </div>

      {/* Share breakdown */}
      {totalShares > 0 && (
        <div style={{ ...CARD, display: "flex", gap: "16px", flexWrap: "wrap" }}>
          <div style={{ fontSize: "10px", fontWeight: 600, color: "#94a3b8", textTransform: "uppercase", letterSpacing: "0.07em", width: "100%", marginBottom: "4px" }}>Share Breakdown</div>
          {[
            ["Accepted", totalAccepted, "#22ff88"],
            ["Rejected", totalRejected, "#ff0080"],
            ["Stale",    totalStale,    "#fbbf24"],
          ].map(([label, val, color]) => (
            <div key={label} style={{ display: "flex", flexDirection: "column", gap: "2px", flex: "1 1 80px" }}>
              <div style={{ fontSize: "16px", fontWeight: 700, color }}>{val.toLocaleString()}</div>
              <div style={{ fontSize: "10px", color: "#94a3b8" }}>{label}</div>
              {totalShares > 0 && (
                <div style={{ height: "3px", background: "rgba(255,255,255,0.06)", borderRadius: "2px", overflow: "hidden", marginTop: "4px" }}>
                  <div style={{ height: "100%", width: `${(val/totalShares*100).toFixed(1)}%`, background: color, borderRadius: "2px" }} />
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Per-coin cards */}
      {Object.keys(coins).length > 0 ? (
        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
          {Object.entries(coins).map(([symbol, data]) => (
            <CoinCard key={symbol} symbol={symbol} engineData={data} nodeData={nodes?.[symbol.toLowerCase()]} engineOnline={engineOnline} miners={miners} />
          ))}
        </div>
      ) : (
        <div style={{ ...CARD, display: "flex", alignItems: "center", justifyContent: "center", padding: "20px", color: "#334155", fontSize: "12px" }}>
          No active coins — install a ForgeNX coin app to get started
        </div>
      )}
    </div>
  )
}

// ── Workers tab ──────────────────────────────────────────────────────────────
function EngineWorkers({ miners, loading, engineOnline, nodes }) {
  const minerList = Object.entries(miners).flatMap(([coin, list]) =>
    (list ?? []).map(m => ({
      ...m,
      coin,
      name: (m.worker_name ?? m.name ?? m.worker ?? "").split(".").pop() || (m.worker_name ?? m.name ?? m.worker ?? "—"),
    }))
  )
  const [shares48h, setShares48h] = useState({})
  useEffect(() => {
    // Fetch 48hr shares for all coins
    const symbols = Object.keys(miners)
    Promise.all(symbols.map(sym => {
      const coinId = `forge${sym.toLowerCase()}`
      return fetch(`/api/apps/${coinId}/worker-shares-48h`)
        .then(r => r.ok ? r.json() : { workers: {} })
        .then(j => j.workers ?? {})
        .catch(() => ({}))
    })).then(results => {
      const merged = {}
      results.forEach(r => Object.assign(merged, r))
      setShares48h(merged)
    })
  }, [miners])
  const fmtDiff = (d) => {
    if (!d || d === 0) return "—"
    if (d >= 1e12) return `${(d/1e12).toFixed(2)} T`
    if (d >= 1e9)  return `${(d/1e9).toFixed(2)} G`
    if (d >= 1e6)  return `${(d/1e6).toFixed(2)} M`
    if (d >= 1e3)  return `${(d/1e3).toFixed(2)} K`
    return d.toFixed(0)
  }
  const fmtConnected = (ts) => {
    if (!ts) return "—"
    const s = Math.floor((Date.now() - new Date(ts).getTime()) / 1000)
    if (s < 60)    return `${s}s`
    if (s < 3600)  return `${Math.floor(s/60)}m`
    if (s < 86400) return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`
    return `${Math.floor(s/86400)}d ${Math.floor((s%86400)/3600)}h`
  }

  if (!engineOnline && Object.keys(miners).length === 0) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", flexDirection: "column", gap: "8px" }}>
      <WifiOff size={24} color="#ff0080" />
      <div style={{ fontSize: "13px", color: "#475569" }}>ForgeNX Engine is offline</div>
    </div>
  )

  // Get stratum URLs from active nodes
  const stratumUrls = Object.entries(nodes ?? {}).map(([sym, n]) => ({
    symbol: sym.toUpperCase(),
    port: n.stratum_port ?? null,
  })).filter(n => n.port)

  if (minerList.length === 0) return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px", padding: "10px 0" }}>
      <div style={{ ...CARD, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: "12px", padding: "32px 16px" }}>
        <Users size={28} color={ENG.color} strokeWidth={1.5} />
        <div style={{ fontSize: "14px", fontWeight: 600, color: "#f1f5f9" }}>No miners connected</div>
        <div style={{ fontSize: "12px", color: "#cbd5e1", textAlign: "center" }}>
          Connect your miner using the Stratum URL below
        </div>
        <div style={{ fontSize: "11px", color: "#cbd5e1", textAlign: "center" }}>Click the address to copy</div>
        {stratumUrls.length > 0 ? (
          <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
            {stratumUrls.map(({ symbol, port }) => (
              <ClickCopy key={symbol} text={`stratum+tcp://${window.location.hostname}:${port}`}>
                <span style={{ color: ENG.color, fontSize: "13px" }}>stratum+tcp://</span><span style={{ color: "#f1f5f9", fontSize: "13px" }}>{window.location.hostname}</span><span style={{ color: ENG.color, fontSize: "13px" }}>:{port}</span><span style={{ color: "#cbd5e1", fontSize: "10px", marginLeft: "8px" }}>({symbol})</span>
              </ClickCopy>
            ))}
          </div>
        ) : (
          <div style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "8px", padding: "8px 12px", fontFamily: "monospace", fontSize: "12px", color: ENG.color }}>
            stratum+tcp://&lt;your-server-ip&gt;:&lt;port&gt;
          </div>
        )}
        <div style={{ fontSize: "11px", color: "#94a3b8" }}>Username: your payout address · Password: anything</div>
      </div>
    </div>
  )

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px", padding: "10px 0" }}>
      <div style={{ ...CARD, overflow: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px", minWidth: "900px" }}>
          <thead>
            <tr>
              {["Worker","Device","IP Address","Mining","Hashrate","Difficulty","Connected","Best Session Diff","Shares (session)","Shares (48h)","Last Share"].map(h => (
                <th key={h} style={{ textAlign: "left", color: "#94a3b8", fontWeight: 600, textTransform: "uppercase", fontSize: "9px", letterSpacing: "0.07em", padding: "0 8px 8px 0", borderBottom: `1px solid ${ENG.colorBorder}`, whiteSpace: "nowrap" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {minerList.map((m, i) => {
              const name = m.name ?? m.worker ?? "—"
              const s48 = shares48h[name] ?? null
              return (
                <tr key={i}>
                  <td style={{ padding: "7px 8px 7px 0", color: "#f1f5f9", fontWeight: 600, borderBottom: "1px solid rgba(255,255,255,0.05)", whiteSpace: "nowrap" }}>{name}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#e2e8f0", borderBottom: "1px solid rgba(255,255,255,0.05)", whiteSpace: "nowrap" }}>{m.vendor ? m.vendor.charAt(0).toUpperCase() + m.vendor.slice(1) : "—"}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#94a3b8", borderBottom: "1px solid rgba(255,255,255,0.05)", whiteSpace: "nowrap", fontFamily: "monospace", fontSize: "11px" }}>{m.remote_addr ? m.remote_addr.split(":")[0] : "—"}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: ENG.color, fontWeight: 600, borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{m.coin ?? "—"}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#f1f5f9", fontWeight: 600, borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{formatHashrate((m.hashrate_15m ?? m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12)} <span style={{fontSize:"10px",color:"#94a3b8",fontWeight:400}}>/ {formatHashrate((m.hashrate_5m ?? m.hashrate_1m ?? m.hashrate ?? 0) * 1e12)}</span></td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#e2e8f0", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{fmtDiff(m.difficulty)}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#94a3b8", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{fmtConnected(m.connected_at)}</td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#e2e8f0", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{fmtDiff(m.best_difficulty)}</td>
                  <td style={{ padding: "7px 8px 7px 0", borderBottom: "1px solid rgba(255,255,255,0.05)", whiteSpace: "nowrap" }}>
                    <span style={{ color: "#22ff88" }}>{(m.shares_accepted ?? 0).toLocaleString()}</span>
                    <span style={{ color: "#475569" }}> / </span>
                    <span style={{ color: (m.shares_rejected ?? 0) > 0 ? "#f87171" : "#475569" }}>{(m.shares_rejected ?? 0).toLocaleString()}</span>
                    <span style={{ color: "#475569" }}> / </span>
                    <span style={{ color: (m.shares_stale ?? 0) > 0 ? "#f59e0b" : "#475569" }}>{(m.shares_stale ?? 0).toLocaleString()}</span>
                  </td>
                  <td style={{ padding: "7px 8px 7px 0", borderBottom: "1px solid rgba(255,255,255,0.05)", whiteSpace: "nowrap" }}>
                    {s48 ? <>
                      <span style={{ color: "#22ff88" }}>{s48.valid.toLocaleString()}</span>
                      <span style={{ color: "#475569" }}> / </span>
                      <span style={{ color: s48.invalid > 0 ? "#f87171" : "#475569" }}>{s48.invalid.toLocaleString()}</span>
                      <span style={{ color: "#475569" }}> / </span>
                      <span style={{ color: s48.stale > 0 ? "#f59e0b" : "#475569" }}>{(s48.stale ?? 0).toLocaleString()}</span>
                    </> : <span style={{ color: "#475569" }}>—</span>}
                  </td>
                  <td style={{ padding: "7px 8px 7px 0", color: "#94a3b8", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>{m.last_share ?? "—"}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Nodes tab ─────────────────────────────────────────────────────────────────
function EngineNodes({ nodes, stats }) {
  const nodeList = Object.entries(nodes ?? {})

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px", padding: "10px 0" }}>
      {nodeList.length === 0 ? (
        <div style={{ ...CARD, display: "flex", alignItems: "center", justifyContent: "center", padding: "24px", color: "#334155", fontSize: "12px" }}>
          No coin nodes installed — install a ForgeNX coin app to get started
        </div>
      ) : (
        nodeList.map(([sym, node]) => {
          const coinData = stats?.coins?.[sym.toUpperCase()]
          const total = coinData ? (coinData.shares_accepted ?? 0) + (coinData.shares_rejected ?? 0) + (coinData.shares_stale ?? 0) : 0
          const acceptPct = total > 0 ? ((coinData.shares_accepted / total) * 100).toFixed(1) : "—"
          return (
          <div key={sym} style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
          <div style={{ ...CARD, display: "flex", gap: "12px", alignItems: "center", flexWrap: "wrap" }}>
            <img src={`/nodes/${sym.toUpperCase()}.png`} alt={sym}
              style={{ width: "32px", height: "32px", borderRadius: "8px", objectFit: "contain",
                filter: node.status === "online" ? `drop-shadow(0 0 6px ${ENG.color}88)` : "none" }}
              onError={e => { e.target.style.display = "none" }} />
            <div style={{ flex: 1, flex: "1 1 220px", minWidth: 0 }}>
              <div style={{ fontSize: "13px", fontWeight: 700, color: "#f1f5f9", marginBottom: "2px" }}>Forge{sym.toUpperCase()}</div>
              <div style={{ fontSize: "11px", color: "#94a3b8" }}>{(() => {
                const raw = node.version ?? ""
                const appVer = node.app_version ? `v${node.app_version}` : null
                const match = raw.match(/([A-Za-z ]+)[:\/]([0-9]+\.[0-9]+\.[0-9]+)/)
                const nodeName = match ? match[1].trim() : raw.replace(/\//g, "").trim()
                const nodeVer  = match ? `v${match[2]}` : null
                const parts = [appVer, nodeName && nodeVer ? `${nodeName} ${nodeVer}` : (nodeName || nodeVer)].filter(Boolean)
                return parts.join(" · ") || "—"
              })()}</div>
            </div>
            <span style={{ fontSize: "10px", fontWeight: 600,
              color: node.status === "online" ? "#22ff88" : "#ff0080",
              background: node.status === "online" ? "rgba(34,255,136,0.1)" : "rgba(255,0,128,0.08)",
              border: `1px solid ${node.status === "online" ? "rgba(34,255,136,0.3)" : "rgba(255,0,128,0.3)"}`,
              borderRadius: "999px", padding: "2px 10px" }}>
              {node.status === "online" ? "Online" : "Offline"}
            </span>
            <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
              {[
                ["P2P", node.p2p_port],
                ["RPC", node.rpc_port],
                ["ZMQ", node.zmq_port],
                ["Stratum", node.stratum_port],
              ].filter(([, v]) => v).map(([label, val]) => (
                <div key={label} style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
                  <span style={{ fontSize: "9px", color: "#475569", textTransform: "uppercase", letterSpacing: "0.07em" }}>{label}</span>
                  <span style={{ fontSize: "12px", color: "#e2e8f0", fontWeight: 500, fontFamily: "monospace" }}>{val}</span>
                </div>
              ))}
            </div>
          </div>
          {coinData && (
            <div style={{ background: "rgba(2,6,17,0.7)", border: `1px solid ${ENG.colorBorder}`, borderRadius: "10px", padding: "10px 14px" }}>
              <div style={{ fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.07em", color: "#94a3b8", marginBottom: "8px" }}>Session Shares</div>
              <div style={{ display: "flex", flexDirection: "column", gap: "0" }}>
                {[
                  ["Shares accepted", coinData.shares_accepted ?? 0, "#22ff88"],
                  ["Shares rejected", coinData.shares_rejected ?? 0, "#ff0080"],
                  ["Shares stale",    coinData.shares_stale    ?? 0, "#fbbf24"],
                  ["Blocks found",    coinData.blocks_found    ?? 0, ENG.color],
                ].map(([label, val, color]) => (
                  <div key={label} style={{ display: "flex", justifyContent: "space-between", fontSize: "12px", padding: "4px 0", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                    <span style={{ color: "#cbd5e1" }}>{label}</span>
                    <span style={{ color, fontWeight: 500 }}>{val.toLocaleString()}</span>
                  </div>
                ))}
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: "12px", padding: "4px 0" }}>
                  <span style={{ color: "#cbd5e1" }}>Share validity</span>
                  <span style={{ color: acceptPct !== "—" ? "#22ff88" : "#94a3b8", fontWeight: 500 }}>{acceptPct !== "—" ? `${acceptPct}%` : "—"}</span>
                </div>
              </div>
            </div>
          )}
          </div>
        )})
      )}
    </div>
  )
}

// ── Main export ───────────────────────────────────────────────────────────────
// ── Engine Settings Tab ──────────────────────────────────────────────────
function ClickCopyPubkey({ text }) {
  const [ok, setOk] = useState(false)
  const copy = () => {
    if (!text) return
    const finish = () => { setOk(true); setTimeout(() => setOk(false), 2000) }
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(finish).catch(() => fallback())
    } else {
      fallback()
    }
    function fallback() {
      const el = document.createElement("textarea"); el.value = text
      el.style.cssText = "position:fixed;top:-9999px;opacity:0"
      document.body.appendChild(el); el.select()
      try { document.execCommand("copy"); finish() } catch {}
      document.body.removeChild(el)
    }
  }
  return ok
    ? <span style={{ fontFamily: "monospace", fontSize: "10px", color: "#22ff88", cursor: "pointer", wordBreak: "break-all", lineHeight: "1.6" }} onClick={copy}>✓ Copied!</span>
    : <span style={{ fontFamily: "monospace", fontSize: "10px", color: "#22ff88", cursor: "pointer", wordBreak: "break-all", lineHeight: "1.6", display: "block" }} onClick={copy}>{text}</span>
}


function EngineSettingsTab() {
  const [coins, setCoins] = useState({})
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    // Fetch SV2 key info from each known coin's settings endpoint
    const KNOWN_COINS = ["forgebch"]
    Promise.all(
      KNOWN_COINS.map(id =>
        fetch(`/api/apps/${id}/settings`)
          .then(r => r.json())
          .then(s => ({ id, symbol: id.replace("forge","").toUpperCase(), s }))
          .catch(() => null)
      )
    ).then(results => {
      const m = {}
      results.forEach(r => { if (r) m[r.symbol] = r.s })
      setCoins(m)
      setLoading(false)
    })
  }, [])

  const cardStyle = { background: "rgba(2,6,17,0.55)", border: "1px solid rgba(0,229,255,0.15)", borderRadius: "12px", padding: "16px" }
  const titleStyle = { fontSize: "11px", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "#e2e8f0", marginBottom: "12px", borderLeft: "3px solid #00e5ff", paddingLeft: "8px" }
  const rowStyle = { display: "flex", justifyContent: "space-between", alignItems: "flex-start", padding: "8px 0", borderBottom: "1px solid rgba(0,229,255,0.08)", gap: "12px" }
  const lblStyle = { color: "#94a3b8", fontSize: "12px", flexShrink: 0 }
  const valStyle = { color: "#e2e8f0", fontWeight: 500, fontSize: "12px", textAlign: "right" }

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "120px", color: "#00e5ff", fontSize: "13px", gap: "10px" }}>
      <div style={{ width: "16px", height: "16px", border: "2px solid rgba(0,229,255,0.3)", borderTopColor: "#00e5ff", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
      Loading…
      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )

  return (
    <div style={{ padding: "12px", display: "flex", flexDirection: "column", gap: "12px", overflowY: "auto" }}>
      {/* Authority Keypair */}
      <div style={cardStyle}>
        <div style={titleStyle}>Authority Keypair</div>
        <div style={{ fontSize: "11px", color: "#64748b", marginBottom: "12px", lineHeight: "1.5" }}>
          One authority keypair per pool — its public key is what miners paste into their <span style={{ fontFamily: "monospace", color: "#00e5ff", fontSize: "10px" }}>sv2_auth_pk</span> field to verify server identity.
        </div>
        {Object.entries(coins).map(([symbol, s]) => (
          <div key={symbol} style={{ marginBottom: "12px" }}>
            <div style={{ fontSize: "11px", fontWeight: 600, color: "#00e5ff", marginBottom: "8px" }}>{symbol}</div>
            <div style={rowStyle}>
              <span style={lblStyle}>Status</span>
              <span style={{ ...valStyle, color: s.sv2AuthorityPubkey ? "#22ff88" : "#ff0080", fontWeight: 700 }}>
                {s.sv2AuthorityPubkey ? "● Loaded" : "● Not generated"}
              </span>
            </div>
            {s.sv2AuthorityPubkey && (
              <div style={{ marginTop: "8px" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "6px" }}>
                  <div style={{ fontSize: "11px", color: "#94a3b8" }}>Authority Public Key</div>
                  <div style={{ fontSize: "9px", color: "#cbd5e1" }}>(click to copy)</div>
                </div>
                <ClickCopyPubkey text={s.sv2AuthorityPubkey} />
                <div style={{ fontSize: "10px", color: "#475569", marginTop: "6px" }}>
                  Paste this into your miner's sv2_auth_pk field to verify server identity
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Per-Variant Static Keys */}
      <div style={cardStyle}>
        <div style={titleStyle}>Per-Variant Static Keys</div>
        <div style={{ fontSize: "11px", color: "#64748b", marginBottom: "12px", lineHeight: "1.5" }}>
          Per-variant static keypairs are used during the Noise handshake to establish the encrypted session. The EllSwift encoding changes every restart by design (BIP-324) — miners don't interact with these directly.
        </div>
        {Object.entries(coins).map(([symbol, s]) => (
          <div key={symbol} style={{ ...rowStyle }}>
            <div>
              <span style={{ color: "#e2e8f0", fontSize: "12px", fontWeight: 600 }}>{symbol}</span>
              <div style={{ fontSize: "10px", color: "#475569", marginTop: "2px" }}>bip324 variant</div>
            </div>
            <span style={{ ...valStyle, color: s.sv2Enabled ? "#22ff88" : "#ff0080", fontWeight: 700 }}>
              {s.sv2Enabled ? "● Loaded" : "● Disabled"}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

function EngineLogsTab() {
  const [logs, setLogs]     = useState("Loading logs…")
  const [tail, setTail]     = useState(100)
  const [live, setLive]     = useState(false)
  const ref                 = useRef(null)
  const liveRef             = useRef(null)
  const [copyOk, setCopyOk] = useState(false)
  const [userScrolled, setUserScrolled] = useState(false)
  const userScrolledRef = useRef(false)
  const logsRef = useRef("")
  const isProgrammaticScroll = useRef(false)
  const handleScroll = () => {
    if (!ref.current) return
    if (isProgrammaticScroll.current) return
    const { scrollTop, scrollHeight, clientHeight } = ref.current
    const atBottom = scrollHeight - scrollTop - clientHeight < 30
    userScrolledRef.current = !atBottom
    setUserScrolled(!atBottom)
  }
  const resumeScroll = () => {
    userScrolledRef.current = false
    setUserScrolled(false)
    if (ref.current) { isProgrammaticScroll.current = true; ref.current.scrollTop = ref.current.scrollHeight; setTimeout(() => { isProgrammaticScroll.current = false }, 100) }
  }
  const copyLogs = () => {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(logsRef.current).catch(() => fallback())
    } else { fallback() }
    function fallback() {
      const el = document.createElement("textarea")
      el.value = logsRef.current
      el.setAttribute("readonly", "")
      el.style.cssText = "position:absolute;left:-9999px;top:0"
      document.body.appendChild(el)
      const selected = document.getSelection().rangeCount > 0 ? document.getSelection().getRangeAt(0) : false
      el.select()
      el.setSelectionRange(0, 99999)
      const success = document.execCommand("copy")
      if (success) { setCopyOk(true); setTimeout(() => setCopyOk(false), 1600) }
      document.body.removeChild(el)
      if (selected) { document.getSelection().removeAllRanges(); document.getSelection().addRange(selected) }
    }
  }

  const fetchLogs = (silent = false) => {
    if (!silent) setLogs("Loading…")
    fetch(`/api/engine/logs?tail=${tail}`)
      .then(r => r.json())
      .then(data => {
        setLogs(data.success ? (data.logs || "No log output.") : "Failed to fetch logs.")
        logsRef.current = data.success ? (data.logs || "No log output.") : "Failed to fetch logs."
        if (!userScrolledRef.current) { setTimeout(() => { if (ref.current) { isProgrammaticScroll.current = true; ref.current.scrollTop = ref.current.scrollHeight; setTimeout(() => { isProgrammaticScroll.current = false }, 100) } }, 50) }
      })
      .catch(() => setLogs("Could not connect to log API."))
  }

  useEffect(() => { fetchLogs() }, [tail])

  useEffect(() => {
    if (live) {
      fetchLogs(true)
      liveRef.current = setInterval(() => fetchLogs(true), 2000)
    } else {
      if (liveRef.current) clearInterval(liveRef.current)
    }
    return () => { if (liveRef.current) clearInterval(liveRef.current) }
  }, [live, tail])

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "8px", flex: 1, minHeight: 0, padding: "10px 0" }}>
      <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
        <select value={tail} onChange={e => { setTail(+e.target.value); setLive(false) }}
          style={{ background: "rgba(2,6,17,0.7)", border: "1px solid rgba(255,255,255,0.16)", borderRadius: "6px", color: "#94a3b8", padding: "4px 8px", fontSize: "12px" }}>
          {[50,100,200,500].map(v => <option key={v} value={v}>Last {v} lines</option>)}
        </select>
        <button onClick={() => setLive(v => !v)} style={{
          padding: "4px 12px", borderRadius: "7px", fontSize: "12px", fontWeight: 600,
          cursor: "pointer", fontFamily: "inherit", transition: "all 0.15s",
          background: live ? "rgba(0,229,255,0.1)" : "rgba(255,0,128,0.08)",
          border: live ? "1px solid rgba(0,229,255,0.3)" : "1px solid rgba(255,0,128,0.3)",
          color: live ? "#00e5ff" : "#ff0080",
        }}>{live ? "● Live" : "○ Live"}</button>
        <button onClick={() => fetchLogs()} disabled={live}
          style={{ padding: "4px 12px", borderRadius: "7px", background: live ? "rgba(255,255,255,0.03)" : "rgba(0,229,255,0.08)", border: `1px solid ${live ? "rgba(255,255,255,0.08)" : "rgba(0,229,255,0.25)"}`, color: live ? "#475569" : "#00e5ff", fontSize: "12px", fontWeight: 600, cursor: live ? "default" : "pointer", fontFamily: "inherit" }}>Refresh</button>
        <button onClick={copyLogs}
          style={{ padding: "4px 12px", borderRadius: "7px", background: "rgba(255,255,255,0.05)", border: `1px solid ${copyOk ? "rgba(0,229,255,0.4)" : "rgba(255,255,255,0.16)"}`, color: copyOk ? "#00e5ff" : "#e2e8f0", fontSize: "12px", fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>{copyOk ? "✓ Copied" : "Copy"}</button>
        {live && <span style={{ fontSize: "10px", color: "#64748b" }}>Updating every 2s</span>}
      </div>
      <div style={{ position: "relative", flex: 1, minHeight: 0 }}>
        <div ref={ref} onScroll={handleScroll} style={{ height: "100%", background: "#010408", border: `1px solid ${live ? "rgba(0,229,255,0.3)" : "rgba(255,255,255,0.1)"}`, borderRadius: "10px", padding: "12px 14px", fontFamily: "monospace", fontSize: "11px", lineHeight: "1.7", color: "#94a3b8", overflowY: "auto", whiteSpace: "pre-wrap", wordBreak: "break-all", minHeight: "150px", transition: "border-color 0.3s" }}>
          {logs}
        </div>
        {live && userScrolled && (
          <div onClick={resumeScroll} style={{ position: "absolute", bottom: "12px", left: "50%", transform: "translateX(-50%)", background: "rgba(2,6,17,0.85)", backdropFilter: "blur(6px)", border: "1px solid rgba(0,229,255,0.5)", borderRadius: "999px", padding: "6px 16px", fontSize: "11px", fontWeight: 600, color: "#00e5ff", cursor: "pointer", whiteSpace: "nowrap", zIndex: 10, boxShadow: "0 2px 12px rgba(0,0,0,0.5)" }}>
            ↓ Auto-scroll paused — Click to resume
          </div>
        )}
      </div>
    </div>
  )
}

// ── Tab Bar ────────────────────────────────────────────────────────────────
const TabBar = ({ tabs, active, onSelect }) => (
  <div style={{ display: "flex", borderBottom: "1px solid rgba(255,255,255,0.07)", flexShrink: 0, overflowX: "auto" }}>
    {tabs.map(tab => (
      <button key={tab} onClick={() => onSelect(tab)} style={{
        padding: "10px 18px", fontSize: "13px",
        fontWeight: active === tab ? "600" : "400",
        color: active === tab ? "#00e5ff" : "#94a3b8",
        background: "transparent", border: "none",
        borderBottom: active === tab ? "2px solid #00e5ff" : "2px solid transparent",
        cursor: "pointer", transition: "all 0.15s",
        fontFamily: "inherit", marginBottom: "-1px", whiteSpace: "nowrap",
      }}>
        {tab}
      </button>
    ))}
  </div>
)

// ── Stat Chip ──────────────────────────────────────────────────────────────
const StatChip = ({ icon: Icon, label, value, color = "#00e5ff" }) => (
  <div style={{
    background: "rgba(2,6,17,0.6)", border: "1px solid rgba(255,255,255,0.07)",
    borderRadius: "14px", padding: "14px 18px",
    display: "flex", alignItems: "center", gap: "12px",
    flex: 1, minWidth: "120px",
  }}>
    <div style={{
      width: "34px", height: "34px", borderRadius: "10px", flexShrink: 0,
      background: `${color}18`, border: `1px solid ${color}30`,
      display: "flex", alignItems: "center", justifyContent: "center",
    }}>
      <Icon size={15} color={color} strokeWidth={1.5} />
    </div>
    <div>
      <div style={{ fontSize: "11px", fontWeight: 600, color: "#94a3b8", marginBottom: "3px" }}>{label}</div>
      <div style={{ fontSize: "16px", fontWeight: "600", color: "#f1f5f9" }}>{value}</div>
    </div>
  </div>
)

// ── Info Field Row ─────────────────────────────────────────────────────────
const InfoField = ({ label, value, isLink = false, mono = false, icon: Icon }) => (
  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", padding: "7px 0", borderBottom: "1px solid rgba(255,255,255,0.1)", gap: "12px" }}>
    <div style={{ display: "flex", alignItems: "center", gap: "5px", flexShrink: 0, paddingTop: "1px" }}>
      {Icon && <Icon size={10} color="#94a3b8" strokeWidth={1.5} />}
      <span style={{ fontSize: "11px", fontWeight: 600, color: "#94a3b8" }}>{label}</span>
    </div>
    {isLink && value ? (
      <a href={value} target="_blank" rel="noopener noreferrer"
        style={{ fontSize: "12px", color: "#00e5ff", textDecoration: "none", textAlign: "right", display: "flex", alignItems: "center", gap: "4px", wordBreak: "break-all" }}>
        {value.replace("https://", "").replace("http://", "")}
        <ExternalLink size={10} style={{ flexShrink: 0 }} />
      </a>
    ) : (
      <div style={{ fontSize: "12px", color: "#e2e8f0", textAlign: "right", fontFamily: mono ? "monospace" : "inherit", wordBreak: "break-all" }}>{value || "—"}</div>
    )}
  </div>
)



// ── Information Tab ──────────────────────────────────────────────────────────
function EngineInformationTab() {
  const [engineApp, setEngineApp] = useState(null)
  const [actionLoading, setActionLoading] = useState(null)
  const [actionMsg, setActionMsg] = useState("")
  const [actionMsgColor, setActionMsgColor] = useState("#94a3b8")

  const showMsg = (msg, duration = 3000, color = "#94a3b8") => {
    setActionMsg(msg); setActionMsgColor(color)
    if (duration) setTimeout(() => setActionMsg(""), duration)
  }

  const [forgenxdAvailable, setForgenxdAvailable] = useState(false)

  useEffect(() => {
    // Try forgenxd store API first (ForgeNX platform)
    fetch('/api/forgenx/apps')
      .then(r => {
        if (!r.ok) throw new Error('not available')
        return r.json()
      })
      .then(data => {
        const app = (data.apps || []).find(a => a.id === 'forgenx-engine')
        if (app) { setEngineApp(app); setForgenxdAvailable(true) }
      })
      .catch(() => {
        // Fallback: use /stats for basic info (Umbrel OS or standalone)
        setForgenxdAvailable(false)
        fetch('/stats').then(r => r.json()).then(data => {
          setEngineApp({
            name: 'ForgeNX Engine',
            description: 'Multi-coin Stratum mining engine',
            developer: 'ForgeNX',
            category: 'bitcoin',
            id: 'forgenx-engine',
            version: null,
            installedVersion: null,
          })
        }).catch(() => {})
      })
  }, [])

  const handleAction = async (action) => {
    setActionLoading(action)
    const msgs = { start: "Starting engine…", stop: "Stopping engine…", restart: "Restarting engine…" }
    const colors = { start: "#22c55e", stop: "#ff0080", restart: "#ff9a1f" }
    showMsg(msgs[action], 0, colors[action])
    try {
      const res = await fetch(`/api/apps/forgenx-engine/${action}`, { method: "POST" })
      const data = await res.json()
      if (!data.success && !data.status) { showMsg(`Error: ${data.error}`); setActionLoading(null); return }
      setTimeout(() => { showMsg(`Engine ${action} complete`, 3000, colors[action]); setActionLoading(null) }, 3000)
    } catch (e) { showMsg(`Failed: ${e.message}`); setActionLoading(null) }
  }

  const hasUpdate = engineApp?.installedVersion && engineApp?.version &&
    engineApp.installedVersion !== engineApp.version

  if (!engineApp) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#1e293b", fontSize: "13px" }}>
      Loading engine info…
    </div>
  )

  return (
    <div style={{ display: "flex", gap: "0", marginTop: "0", padding: "12px", overflow: "visible", alignItems: "flex-start" }}>
      {/* Left 50% */}
      <div style={{ flex: "0 0 50%", display: "flex", flexDirection: "column", gap: "4px", overflowY: "auto", paddingRight: "12px", paddingTop: "0" }}>
        {/* Header */}
        <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
          <img src="/nodes/Engine.png" alt="ForgeNX Engine"
            style={{ width: "96px", height: "96px", borderRadius: "16px", objectFit: "contain", flexShrink: 0 }}
            onError={e => { e.target.style.display = "none" }} />
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: "14px", fontWeight: "700", color: "#f1f5f9", marginBottom: "4px" }}>{engineApp.name}</div>
            <div style={{ fontSize: "12px", color: "#94a3b8", lineHeight: "1.5" }}>{engineApp.description}</div>
          </div>
        </div>
        {/* Info fields */}
        <div style={{ background: "rgba(2,6,17,0.5)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "10px", padding: "0px 14px", marginTop: "-18px" }}>
          <InfoField label="Description" value={engineApp.longDescription || engineApp.description} icon={Package} />
          <InfoField label="Version"   value={engineApp.installedVersion || engineApp.version} icon={Tag} />
          <InfoField label="Channel"   value={engineApp.channel ?? "stable"} icon={GitBranch} />
          <InfoField label="Developer" value={engineApp.developer} icon={User} />
          <InfoField label="Category"  value={engineApp.category} icon={Box} />
          <InfoField label="Store ID"  value={engineApp.id} mono icon={Hash} />
          <InfoField label="Website"   value={engineApp.website} isLink icon={ExternalLink} />
          <InfoField label="Support"   value={engineApp.support} isLink icon={Headphones} />
        </div>
        {/* Actions — only shown when forgenxd is available (ForgeNX platform) */}
        {forgenxdAvailable && (
        <div style={{ background: "rgba(2,6,17,0.5)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "10px", padding: "10px 14px", marginTop: "8px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "10px" }}>
            <div style={{ fontSize: "10px", color: "#e2e8f0", textTransform: "uppercase", letterSpacing: "0.8px" }}>Actions</div>
            {actionMsg && <div style={{ fontSize: "11px", color: actionMsgColor }}>{actionMsg}</div>}
          </div>
          <div style={{ display: "flex", gap: "18px", flexWrap: "wrap" }}>
            <button onClick={() => handleAction("start")} disabled={actionLoading !== null}
              style={{ display: "flex", alignItems: "center", gap: "6px", padding: "7px 14px", borderRadius: "8px", cursor: actionLoading ? "wait" : "pointer", background: "rgba(34,197,94,0.08)", border: "1px solid rgba(34,197,94,0.3)", color: "#22c55e", fontSize: "12px", fontWeight: "600", fontFamily: "inherit" }}>
              <Play size={12} /> {actionLoading === "start" ? "Starting…" : "Start"}
            </button>
            <button onClick={() => handleAction("stop")} disabled={actionLoading !== null}
              style={{ display: "flex", alignItems: "center", gap: "6px", padding: "7px 14px", borderRadius: "8px", cursor: actionLoading ? "wait" : "pointer", background: "rgba(255,0,128,0.08)", border: "1px solid rgba(255,0,128,0.3)", color: "#ff0080", fontSize: "12px", fontWeight: "600", fontFamily: "inherit" }}>
              <Square size={12} /> {actionLoading === "stop" ? "Stopping…" : "Stop"}
            </button>
            <button onClick={() => handleAction("restart")} disabled={actionLoading !== null}
              style={{ display: "flex", alignItems: "center", gap: "6px", padding: "7px 14px", borderRadius: "8px", cursor: actionLoading ? "wait" : "pointer", background: "rgba(255,154,31,0.08)", border: "1px solid rgba(255,154,31,0.3)", color: "#ff9a1f", fontSize: "12px", fontWeight: "600", fontFamily: "inherit" }}>
              <RotateCcw size={12} /> {actionLoading === "restart" ? "Restarting…" : "Restart"}
            </button>
            {hasUpdate && (
              <button style={{ display: "flex", alignItems: "center", gap: "6px", padding: "7px 14px", borderRadius: "8px", cursor: "pointer", background: "rgba(59,130,246,0.1)", border: "1px solid rgba(59,130,246,0.35)", color: "#93c5fd", fontSize: "12px", fontWeight: "600", fontFamily: "inherit" }}>
                <Download size={12} /> Update v{engineApp.version}
              </button>
            )}
          </div>
        </div>
        )}
      </div>
      {/* Divider */}
      <div style={{ width: "1px", background: "rgba(255,255,255,0.08)", flexShrink: 0, margin: "0 12px" }} />
      {/* Right 50% */}
      <div style={{ flex: "0 0 calc(50% - 25px)", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ fontSize: "12px", color: "#1e293b" }}>Reserved for future use</div>
      </div>
    </div>
  )
}


export default function EngineUI({ activeTab, engineOnline, nodes, initialStats, initialMiners }) {
  const { stats, miners, loading } = useEngineData(engineOnline, initialStats, initialMiners)

  if (activeTab === "Overview")    return <EngineDashboard stats={stats} miners={miners} loading={loading} engineOnline={engineOnline} nodes={nodes} />
  if (activeTab === "Workers")     return <EngineWorkers   miners={miners} loading={loading} engineOnline={engineOnline} nodes={nodes} />
  if (activeTab === "Nodes")       return <EngineNodes     nodes={nodes} stats={stats} />
  if (activeTab === "Settings")    return <EngineSettingsTab />
  if (activeTab === "Information") return <EngineInformationTab />
  if (activeTab === "Logs")        return <EngineLogsTab />
  return null
}
