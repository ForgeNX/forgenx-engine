import { StrictMode, useState, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import EngineUI from './EngineUI.jsx'

function StandaloneEngineUI() {
  const [activeTab, setActiveTab] = useState('Overview')
  const [engineOnline, setEngineOnline] = useState(false)
  const [nodes, setNodes] = useState({})

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await fetch('/api/engine/stats')
        if (res.ok) setEngineOnline(true)
        else setEngineOnline(false)
      } catch { setEngineOnline(false) }
      try {
        // Fetch node data from engine's own coin status endpoints
        // (more accurate than forgenxd's /api/nodes which may be stale)
        const statsRes = await fetch('/stats')
        if (statsRes.ok) {
          const statsData = await statsRes.json()
          const coinSymbols = Object.keys(statsData.coins ?? {})
          const nodeMap = {}
          await Promise.all(coinSymbols.map(async sym => {
            const coinId = 'forge' + sym.toLowerCase()
            try {
              const r = await fetch(`/api/apps/${coinId}/status`)
              if (r.ok) {
                const d = await r.json()
                nodeMap[sym.toLowerCase()] = {
                  ...d.node,
                  stratum_port: d.stratum_port ?? d.node?.stratum_port ?? null,
                  zmq_connected: d.zmq_connected,
                }
              }
            } catch {}
          }))
          if (Object.keys(nodeMap).length > 0) setNodes(nodeMap)
        }
      } catch {}
    }
    fetchStatus()
    const interval = setInterval(fetchStatus, 10000)
    return () => clearInterval(interval)
  }, [])

  const tabs = ['Overview', 'Workers', 'Nodes', 'Settings', 'Information', 'Logs']

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#020611', color: '#e2e8f0', fontFamily: 'Inter, -apple-system, sans-serif' }}>
      {/* Tab bar */}
      <div style={{ display: 'flex', gap: '2px', padding: '8px 12px 0', borderBottom: '1px solid rgba(255,0,180,0.2)', background: 'rgba(2,6,17,0.8)' }}>
        {tabs.map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)} style={{
            padding: '6px 16px', borderRadius: '8px 8px 0 0', border: 'none', cursor: 'pointer',
            background: 'transparent',
            color: activeTab === tab ? '#ff00b4' : '#64748b',
            fontWeight: activeTab === tab ? 600 : 400,
            fontSize: '13px', fontFamily: 'inherit',
            borderBottom: activeTab === tab ? '2px solid #ff00b4' : '2px solid transparent',
          }}>{tab}</button>
        ))}
      </div>
      {/* Content */}
      <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <EngineUI activeTab={activeTab} engineOnline={engineOnline} nodes={nodes} />
      </div>
    </div>
  )
}

const _engineStyle = document.createElement('style')
_engineStyle.textContent = `@keyframes pulse { 0% { transform:scale(.95); box-shadow:0 0 0 0 rgba(0,207,170,.45); } 70% { transform:scale(1); box-shadow:0 0 0 6px rgba(0,207,170,0); } 100% { transform:scale(.95); box-shadow:0 0 0 0 rgba(0,207,170,0); } }`
document.head.appendChild(_engineStyle)

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <StandaloneEngineUI />
  </StrictMode>
)
