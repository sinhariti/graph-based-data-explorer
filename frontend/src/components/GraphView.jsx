import { useEffect, useRef, useCallback } from 'react'
import cytoscape from 'cytoscape'
import dagre from 'cytoscape-dagre'
import './GraphView.css'

// Register dagre layout
cytoscape.use(dagre)

// Node type → color mapping
const TYPE_COLORS = {
    sales_order_headers: '#5DCAA5',
    sales_order_items: '#4ABFA0',
    outbound_delivery_headers: '#4A9FD9',
    outbound_delivery_items: '#3D8BC7',
    billing_document_headers: '#F0A050',
    billing_document_items: '#D99040',
    billing_document_cancellations: '#E05858',
    journal_entry_items: '#F0D050',
    payments: '#50D0A0',
    business_partners: '#7F77DD',
    products: '#D070B8',
    product_descriptions: '#C060A8',
    plants: '#70A0D0',
}

const TYPE_LABELS = {
    sales_order_headers: 'Sales Order',
    sales_order_items: 'Order Item',
    outbound_delivery_headers: 'Delivery',
    outbound_delivery_items: 'Delivery Item',
    billing_document_headers: 'Billing Doc',
    billing_document_items: 'Billing Item',
    billing_document_cancellations: 'Cancellation',
    journal_entry_items: 'Journal Entry',
    payments: 'Payment',
    business_partners: 'Customer',
    products: 'Product',
    product_descriptions: 'Product Desc',
    plants: 'Plant',
}

const CYTOSCAPE_STYLE = [
    // Default node style
    {
        selector: 'node',
        style: {
            'label': 'data(label)',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'font-size': '9px',
            'font-family': 'Inter, sans-serif',
            'color': '#9090b0',
            'text-margin-y': 6,
            'width': 24,
            'height': 24,
            'border-width': 2,
            'border-color': '#ffffff20',
            'background-opacity': 0.9,
            'text-max-width': '80px',
            'text-wrap': 'ellipsis',
            'transition-property': 'background-color, border-color, width, height',
            'transition-duration': '0.2s',
        },
    },
    // Per-type node colors
    ...Object.entries(TYPE_COLORS).map(([type, color]) => ({
        selector: `node[type="${type}"]`,
        style: {
            'background-color': color,
            'border-color': color + '40',
        },
    })),
    // Selected node
    {
        selector: 'node:selected',
        style: {
            'border-width': 3,
            'border-color': '#7c6df0',
            'width': 32,
            'height': 32,
        },
    },
    // Highlighted node (from chat results)
    {
        selector: 'node.highlighted',
        style: {
            'border-width': 4,
            'border-color': '#7c6df0',
            'width': 36,
            'height': 36,
            'z-index': 100,
        },
    },
    // Default edge style
    {
        selector: 'edge',
        style: {
            'width': 1,
            'line-color': '#4a4a7030',
            'target-arrow-color': '#4a4a7050',
            'target-arrow-shape': 'triangle',
            'arrow-scale': 0.6,
            'curve-style': 'bezier',
            'transition-property': 'line-color, width',
            'transition-duration': '0.2s',
        },
    },
    // Edge on hover/connected to selected
    {
        selector: 'edge.connected',
        style: {
            'width': 2,
            'line-color': '#7c6df060',
            'target-arrow-color': '#7c6df080',
        },
    },
    // Hide labels when overlay is off
    {
        selector: 'node.no-label',
        style: {
            'label': '',
        },
    },
]

export default function GraphView({ elements, onNodeClick, highlightedNodes, showOverlay }) {
    const containerRef = useRef(null)
    const cyRef = useRef(null)

    // Initialize Cytoscape
    useEffect(() => {
        if (!containerRef.current || !elements) return

        const cy = cytoscape({
            container: containerRef.current,
            elements: elements,
            style: CYTOSCAPE_STYLE,
            layout: {
                name: 'dagre',
                rankDir: 'LR',
                nodeSep: 30,
                rankSep: 60,
                edgeSep: 10,
                animate: false,
            },
            minZoom: 0.1,
            maxZoom: 4,
            wheelSensitivity: 0.3,
        })

        cyRef.current = cy

        // Node click handler
        cy.on('tap', 'node', (e) => {
            const node = e.target
            const nodeId = node.id()
            const nodeType = node.data('type')

            // Highlight connected edges
            cy.edges().removeClass('connected')
            node.connectedEdges().addClass('connected')

            onNodeClick(nodeId, nodeType)
        })

        // Background click → deselect
        cy.on('tap', (e) => {
            if (e.target === cy) {
                cy.edges().removeClass('connected')
            }
        })

        // Fit to screen
        cy.fit(undefined, 40)

        return () => {
            cy.destroy()
        }
    }, [elements])

    // Handle label visibility
    useEffect(() => {
        if (!cyRef.current) return
        if (showOverlay) {
            cyRef.current.nodes().removeClass('no-label')
        } else {
            cyRef.current.nodes().addClass('no-label')
        }
    }, [showOverlay])

    // Handle highlighted nodes from chat
    useEffect(() => {
        if (!cyRef.current) return
        const cy = cyRef.current

        // Remove previous highlights
        cy.nodes().removeClass('highlighted')

        if (highlightedNodes && highlightedNodes.length > 0) {
            highlightedNodes.forEach((id) => {
                const node = cy.$(`#${CSS.escape(id)}`)
                if (node.length > 0) {
                    node.addClass('highlighted')
                }
            })

            // Try to fit the highlighted nodes in view
            const highlightedEles = cy.nodes('.highlighted')
            if (highlightedEles.length > 0) {
                cy.animate({
                    fit: { eles: highlightedEles, padding: 80 },
                    duration: 600,
                    easing: 'ease-out-cubic',
                })
            }
        }
    }, [highlightedNodes])

    return (
        <div className="graph-view">
            <div ref={containerRef} className="graph-canvas" />

            {/* Legend */}
            <div className="graph-legend">
                {Object.entries(TYPE_LABELS).map(([type, label]) => (
                    <div
                        key={type}
                        className="legend-item"
                        onClick={() => {
                            if (!cyRef.current) return
                            const nodes = cyRef.current.nodes(`[type="${type}"]`)
                            if (nodes.length > 0) {
                                cyRef.current.animate({
                                    fit: { eles: nodes, padding: 60 },
                                    duration: 500,
                                })
                            }
                        }}
                    >
                        <span
                            className="legend-dot"
                            style={{ backgroundColor: TYPE_COLORS[type] }}
                        />
                        <span className="legend-label">{label}</span>
                    </div>
                ))}
            </div>
        </div>
    )
}
