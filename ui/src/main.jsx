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
                  stratum_port: d.node?.stratum_port ?? 3334,
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
      {/* Header + Tab bar combined */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '0', padding: '0 12px 0 16px', borderBottom: '1px solid rgba(255,0,180,0.2)', background: 'rgba(2,6,17,0.8)', flexShrink: 0, height: '42px' }}>
        <span style={{ width: '7px', height: '7px', borderRadius: '50%', flexShrink: 0, marginRight: '7px',
          background: engineOnline ? '#00cfaa' : 'transparent',
          border: engineOnline ? 'none' : '1.5px solid #ff0080',
          boxShadow: engineOnline ? '0 0 6px rgba(0,207,170,0.5)' : '0 0 6px rgba(255,0,128,0.5)',
          animation: engineOnline ? 'pulse 2.2s ease-in-out infinite' : 'none',
          display: 'inline-block' }} />
        <span style={{ fontSize: '13px', fontWeight: 600, color: '#e2e8f0', marginRight: '8px', whiteSpace: 'nowrap' }}>Fleet Overview</span>
        <span style={{ fontSize: '11px', color: engineOnline ? '#00cfaa' : '#ff0080', marginRight: '16px', whiteSpace: 'nowrap' }}>{engineOnline ? 'Online' : 'Offline'}</span>
        <div style={{ display: 'flex', gap: '2px', alignItems: 'flex-end', height: '100%' }}>
          {tabs.map(tab => (
            <button key={tab} onClick={() => setActiveTab(tab)} style={{
              padding: '6px 16px', borderRadius: '8px 8px 0 0', border: 'none', cursor: 'pointer',
              background: 'transparent',
              color: activeTab === tab ? '#ff00b4' : '#64748b',
              fontWeight: activeTab === tab ? 600 : 400,
              fontSize: '13px', fontFamily: 'inherit',
              borderBottom: activeTab === tab ? '2px solid #ff00b4' : '2px solid transparent',
              height: '100%',
            }}>{tab}</button>
          ))}
        </div>
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
