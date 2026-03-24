import { useState, useRef, useEffect } from 'react'
import './ChatPanel.css'

const AGENT_NAME = 'Graph AI'
const AGENT_SUBTITLE = 'Graph Agent'

const WELCOME_MESSAGE = {
    role: 'agent',
    content: 'Hi! I can help you analyze the **Order to Cash** process. Ask me anything about sales orders, deliveries, billing documents, payments, or customers.',
}

export default function ChatPanel({ onResponse, chatHistory, setChatHistory }) {
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)
    const messagesEndRef = useRef(null)

    // Initialize with welcome message
    useEffect(() => {
        if (chatHistory.length === 0) {
            setChatHistory([WELCOME_MESSAGE])
        }
    }, [])

    // Auto-scroll to bottom
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }, [chatHistory, loading])

    const handleSend = async () => {
        const message = input.trim()
        if (!message || loading) return

        // Add user message
        const userMsg = { role: 'user', content: message }
        setChatHistory((prev) => [...prev, userMsg])
        setInput('')
        setLoading(true)

        try {
            // Build history for context
            const history = chatHistory
                .filter((m) => m.role !== 'agent' || m.sql)
                .map((m) => ({
                    question: m.role === 'user' ? m.content : undefined,
                    sql: m.sql || undefined,
                }))
                .filter((h) => h.question || h.sql)

            const apiBase = import.meta.env.VITE_API_URL || ''
            const res = await fetch(`${apiBase}/api/chat`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message, history }),
            })

            const data = await res.json()

            const agentMsg = {
                role: 'agent',
                content: data.answer,
                sql: data.sql || null,
                data: data.data || [],
                nodes: data.nodes || [],
                rejected: data.rejected || false,
            }

            setChatHistory((prev) => [...prev, agentMsg])

            // Notify parent about response (for graph highlighting)
            if (onResponse) {
                onResponse(data)
            }
        } catch (err) {
            const errorMsg = {
                role: 'agent',
                content: 'Sorry, I encountered an error connecting to the server. Please try again.',
                error: true,
            }
            setChatHistory((prev) => [...prev, errorMsg])
        } finally {
            setLoading(false)
        }
    }

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault()
            handleSend()
        }
    }

    return (
        <div className="chat-panel">
            {/* Header */}
            <div className="chat-header">
                <div className="chat-header-info">
                    <h2 className="chat-title">Chat with Graph</h2>
                    <span className="chat-subtitle">Order to Cash</span>
                </div>
            </div>

            {/* Agent info */}
            <div className="chat-agent">
                <div className="agent-avatar">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M12 2L2 7l10 5 10-5-10-5z" />
                        <path d="M2 17l10 5 10-5" />
                        <path d="M2 12l10 5 10-5" />
                    </svg>
                </div>
                <div>
                    <div className="agent-name">{AGENT_NAME}</div>
                    <div className="agent-role">{AGENT_SUBTITLE}</div>
                </div>
            </div>

            {/* Messages */}
            <div className="chat-messages">
                {chatHistory.map((msg, i) => (
                    <div key={i} className={`message message-${msg.role}`}>
                        {msg.role === 'agent' && (
                            <div className="message-avatar">
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                    <path d="M12 2L2 7l10 5 10-5-10-5z" />
                                    <path d="M2 17l10 5 10-5" />
                                    <path d="M2 12l10 5 10-5" />
                                </svg>
                            </div>
                        )}
                        <div className="message-content">
                            <div className="message-text">
                                {msg.content.split('**').map((part, j) =>
                                    j % 2 === 1 ? <strong key={j}>{part}</strong> : part
                                )}
                            </div>

                            {/* SQL details */}
                            {msg.sql && (
                                <details className="message-sql">
                                    <summary>View SQL Query</summary>
                                    <pre><code>{msg.sql}</code></pre>
                                </details>
                            )}

                            {/* Data table */}
                            {msg.data && msg.data.length > 0 && (
                                <details className="message-data">
                                    <summary>View Data ({msg.data.length} rows)</summary>
                                    <div className="data-table-wrapper">
                                        <table className="data-table">
                                            <thead>
                                                <tr>
                                                    {Object.keys(msg.data[0]).map((key) => (
                                                        <th key={key}>{key}</th>
                                                    ))}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {msg.data.slice(0, 20).map((row, ri) => (
                                                    <tr key={ri}>
                                                        {Object.values(row).map((val, vi) => (
                                                            <td key={vi}>{val != null ? String(val) : '—'}</td>
                                                        ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </details>
                            )}

                            {/* Highlighted nodes indicator */}
                            {msg.nodes && msg.nodes.length > 0 && (
                                <div className="message-nodes">
                                    📍 {msg.nodes.length} node{msg.nodes.length > 1 ? 's' : ''} highlighted on graph
                                </div>
                            )}
                        </div>
                    </div>
                ))}

                {/* Loading indicator */}
                {loading && (
                    <div className="message message-agent">
                        <div className="message-avatar">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <path d="M12 2L2 7l10 5 10-5-10-5z" />
                                <path d="M2 17l10 5 10-5" />
                                <path d="M2 12l10 5 10-5" />
                            </svg>
                        </div>
                        <div className="message-content">
                            <div className="typing-indicator">
                                <span></span><span></span><span></span>
                            </div>
                        </div>
                    </div>
                )}

                <div ref={messagesEndRef} />
            </div>

            {/* Input */}
            <div className="chat-input-area">
                <div className="chat-status">
                    <span className="status-dot" />
                    <span>{AGENT_NAME} is awaiting instructions</span>
                </div>
                <div className="chat-input-row">
                    <textarea
                        className="chat-input"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={handleKeyDown}
                        placeholder="Analyze anything"
                        rows={1}
                        disabled={loading}
                    />
                    <button
                        className="chat-send-btn"
                        onClick={handleSend}
                        disabled={!input.trim() || loading}
                    >
                        Send
                    </button>
                </div>
            </div>
        </div>
    )
}
