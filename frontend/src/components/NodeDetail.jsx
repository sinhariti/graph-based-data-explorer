import { useState } from 'react'
import './NodeDetail.css'

// Friendly type names
const TYPE_NAMES = {
    sales_order_headers: 'Sales Order',
    sales_order_items: 'Order Item',
    outbound_delivery_headers: 'Delivery',
    outbound_delivery_items: 'Delivery Item',
    billing_document_headers: 'Billing Document',
    billing_document_items: 'Billing Item',
    billing_document_cancellations: 'Cancellation',
    journal_entry_items: 'Journal Entry',
    payments: 'Payment',
    business_partners: 'Customer',
    products: 'Product',
    product_descriptions: 'Product Description',
    plants: 'Plant',
}

// Key fields to show first (per type)
const PRIORITY_FIELDS = {
    sales_order_headers: ['salesOrder', 'salesOrderType', 'soldToParty', 'totalNetAmount', 'transactionCurrency', 'creationDate', 'overallDeliveryStatus'],
    sales_order_items: ['salesOrder', 'salesOrderItem', 'material', 'requestedQuantity', 'netAmount'],
    billing_document_headers: ['billingDocument', 'billingDocumentType', 'totalNetAmount', 'soldToParty', 'accountingDocument', 'creationDate'],
    outbound_delivery_headers: ['deliveryDocument', 'overallGoodsMovementStatus', 'overallPickingStatus', 'creationDate'],
    journal_entry_items: ['accountingDocument', 'accountingDocumentItem', 'referenceDocument', 'amountInTransactionCurrency', 'customer'],
    payments: ['accountingDocument', 'accountingDocumentItem', 'amountInTransactionCurrency', 'customer', 'clearingDate'],
    business_partners: ['businessPartner', 'businessPartnerName', 'businessPartnerCategory', 'creationDate'],
    products: ['product', 'productType', 'productGroup', 'baseUnit', 'grossWeight'],
}

const MAX_VISIBLE_FIELDS = 10

export default function NodeDetail({ data, onClose }) {
    const [expanded, setExpanded] = useState(false)

    if (!data) return null

    const typeName = TYPE_NAMES[data.node_type] || data.node_type
    const metadata = data.metadata || {}
    const allFields = Object.entries(metadata)

    // Sort: priority fields first, then the rest
    const priorityKeys = PRIORITY_FIELDS[data.node_type] || []
    const sortedFields = [
        ...allFields.filter(([key]) => priorityKeys.includes(key)),
        ...allFields.filter(([key]) => !priorityKeys.includes(key)),
    ]

    const visibleFields = expanded ? sortedFields : sortedFields.slice(0, MAX_VISIBLE_FIELDS)
    const hiddenCount = sortedFields.length - MAX_VISIBLE_FIELDS

    return (
        <div className="node-detail">
            {/* Close button */}
            <button className="node-detail-close" onClick={onClose}>
                ✕
            </button>

            {/* Header */}
            <div className="node-detail-header">
                <h3 className="node-detail-type">{typeName}</h3>
            </div>

            {/* Fields */}
            <div className="node-detail-fields">
                {visibleFields.map(([key, value]) => (
                    <div key={key} className="field-row">
                        <span className="field-key">{formatFieldName(key)}:</span>
                        <span className="field-value">{formatValue(value)}</span>
                    </div>
                ))}

                {!expanded && hiddenCount > 0 && (
                    <button
                        className="field-show-more"
                        onClick={() => setExpanded(true)}
                    >
                        <em>Additional {hiddenCount} fields hidden for readability</em>
                    </button>
                )}

                {expanded && hiddenCount > 0 && (
                    <button
                        className="field-show-more"
                        onClick={() => setExpanded(false)}
                    >
                        <em>Show less</em>
                    </button>
                )}
            </div>

            {/* Connection count */}
            <div className="node-detail-connections">
                <span className="connections-label">Connections:</span>
                <span className="connections-count">{data.connections}</span>
            </div>
        </div>
    )
}

function formatFieldName(key) {
    // camelCase → Title Case
    return key
        .replace(/([A-Z])/g, ' $1')
        .replace(/^./, (s) => s.toUpperCase())
        .trim()
}

function formatValue(value) {
    if (value === null || value === undefined || value === '') return '—'
    if (typeof value === 'boolean') return value ? 'Yes' : 'No'
    if (typeof value === 'object') return JSON.stringify(value)

    // Format ISO dates
    const str = String(value)
    if (str.match(/^\d{4}-\d{2}-\d{2}T/)) {
        return str.replace('T', ' ').replace(/\.\d{3}Z$/, '')
    }

    return str
}
