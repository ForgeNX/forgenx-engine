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
        const res = await fetch('/api/nodes')
        if (res.ok) setNodes(await res.json())
      } catch {}
    }
    fetchStatus()
    const interval = setInterval(fetchStatus, 10000)
    return () => clearInterval(interval)
  }, [])

  const tabs = ['Overview', 'Workers', 'Nodes', 'Settings']

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#020611', color: '#e2e8f0', fontFamily: 'Inter, -apple-system, sans-serif' }}>
      {/* Tab bar */}
      <div style={{ display: 'flex', gap: '2px', padding: '8px 12px 0', borderBottom: '1px solid rgba(255,0,180,0.2)', background: 'rgba(2,6,17,0.8)' }}>
        {tabs.map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)} style={{
            padding: '6px 16px', borderRadius: '8px 8px 0 0', border: 'none', cursor: 'pointer',
            background: activeTab === tab ? 'rgba(255,0,180,0.15)' : 'transparent',
            color: activeTab === tab ? '#ff00b4' : '#64748b',
            fontWeight: activeTab === tab ? 600 : 400,
            fontSize: '13px', fontFamily: 'inherit',
            borderBottom: activeTab === tab ? '2px solid #ff00b4' : '2px solid transparent',
          }}>{tab}</button>
        ))}
      </div>
      {/* Content */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <EngineUI activeTab={activeTab} engineOnline={engineOnline} nodes={nodes} />
      </div>
    </div>
  )
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <StandaloneEngineUI />
  </StrictMode>
)
