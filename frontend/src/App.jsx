import { useState, useEffect, useCallback } from 'react'
import GraphView from './components/GraphView'
import ChatPanel from './components/ChatPanel'
import NodeDetail from './components/NodeDetail'
import './App.css'

function App() {
  const [graphData, setGraphData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [selectedNode, setSelectedNode] = useState(null)
  const [highlightedNodes, setHighlightedNodes] = useState([])
  const [graphStats, setGraphStats] = useState(null)
  const [chatHistory, setChatHistory] = useState([])
  const [showOverlay, setShowOverlay] = useState(true)

  // Fetch the full graph on mount
  useEffect(() => {
    const fetchGraph = async () => {
      try {
        const [graphRes, statsRes] = await Promise.all([
          fetch('/api/graph/full'),
          fetch('/api/graph/stats'),
        ])
        const graphJson = await graphRes.json()
        const statsJson = await statsRes.json()
        setGraphData(graphJson.elements)
        setGraphStats(statsJson)
      } catch (err) {
        console.error('Failed to fetch graph:', err)
      } finally {
        setLoading(false)
      }
    }
    fetchGraph()
  }, [])

  // Handle node click
  const handleNodeClick = useCallback(async (nodeId, nodeType) => {
    try {
      const res = await fetch(`/api/graph/node/${encodeURIComponent(nodeId)}?node_type=${encodeURIComponent(nodeType)}`)
      if (res.ok) {
        const data = await res.json()
        setSelectedNode(data)
      }
    } catch (err) {
      console.error('Failed to fetch node detail:', err)
    }
  }, [])

  // Handle chat response (highlight referenced nodes)
  const handleChatResponse = useCallback((response) => {
    if (response.nodes && response.nodes.length > 0) {
      setHighlightedNodes(response.nodes)
      // Clear highlights after 4 seconds
      setTimeout(() => setHighlightedNodes([]), 4000)
    }
  }, [])

  // Close node detail
  const handleCloseDetail = useCallback(() => {
    setSelectedNode(null)
  }, [])

  return (
    <div className="app">
      {/* Header */}
      <header className="app-header">
        <div className="app-header-left">
          <div className="app-logo">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <circle cx="12" cy="12" r="3" />
              <circle cx="4" cy="6" r="2" />
              <circle cx="20" cy="6" r="2" />
              <circle cx="4" cy="18" r="2" />
              <circle cx="20" cy="18" r="2" />
              <line x1="9.5" y1="10.5" x2="5.5" y2="7.5" />
              <line x1="14.5" y1="10.5" x2="18.5" y2="7.5" />
              <line x1="9.5" y1="13.5" x2="5.5" y2="16.5" />
              <line x1="14.5" y1="13.5" x2="18.5" y2="16.5" />
            </svg>
          </div>
          <nav className="app-breadcrumb">
            <span className="breadcrumb-link">Mapping</span>
            <span className="breadcrumb-separator">/</span>
            <span className="breadcrumb-current">Order to Cash</span>
          </nav>
        </div>
        <div className="app-header-right">
          {graphStats && (
            <div className="header-stats">
              <span className="stat">{graphStats.total_nodes} nodes</span>
              <span className="stat-sep">·</span>
              <span className="stat">{graphStats.total_edges} edges</span>
              <span className="stat-sep">·</span>
              <span className="stat">{graphStats.connected_components} components</span>
            </div>
          )}
        </div>
      </header>

      {/* Main content */}
      <div className="app-content">
        {/* Graph area */}
        <div className="graph-area">
          {loading ? (
            <div className="graph-loading">
              <div className="loading-spinner" />
              <p>Loading graph data...</p>
            </div>
          ) : (
            <GraphView
              elements={graphData}
              onNodeClick={handleNodeClick}
              highlightedNodes={highlightedNodes}
              showOverlay={showOverlay}
            />
          )}

          {/* Graph toolbar */}
          <div className="graph-toolbar">
            <button
              className="toolbar-btn"
              onClick={() => setShowOverlay(!showOverlay)}
              title={showOverlay ? "Hide Granular Overlay" : "Show Granular Overlay"}
            >
              {showOverlay ? '🔍 Hide Labels' : '🔍 Show Labels'}
            </button>
          </div>

          {/* Node detail panel */}
          {selectedNode && (
            <NodeDetail
              data={selectedNode}
              onClose={handleCloseDetail}
            />
          )}
        </div>

        {/* Chat panel */}
        <ChatPanel
          onResponse={handleChatResponse}
          chatHistory={chatHistory}
          setChatHistory={setChatHistory}
        />
      </div>
    </div>
  )
}

export default App
