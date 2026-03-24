"""
Database module: SQLite connection and query helpers.
"""

import os
from pathlib import Path
from typing import Optional

# Resolve DB_PATH relative to the project root
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "o2c_data.db"
DB_PATH = Path(os.getenv("DB_PATH", str(DEFAULT_DB_PATH))).resolve()

print(f"DEBUG: Database connecting to {DB_PATH}")

_connection: Optional[sqlite3.Connection] = None


def get_connection() -> sqlite3.Connection:
    """Get or create a singleton SQLite connection."""
    global _connection
    if _connection is None:
        _connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _connection.row_factory = sqlite3.Row
        _connection.execute("PRAGMA journal_mode=WAL")
        _connection.execute("PRAGMA query_only=ON")
    return _connection


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a read-only SQL query and return results as list of dicts."""
    conn = get_connection()
    try:
        cursor = conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise RuntimeError(f"SQL execution error: {e}\nSQL: {sql}")


def get_schema_description() -> str:
    """Generate a human-readable schema description for LLM prompts."""
    conn = get_connection()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('edges', 'graph_nodes') ORDER BY name"
    ).fetchall()

    schema_parts = []
    for (table_name,) in tables:
        # Get column info
        cols = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        col_descriptions = []
        for col in cols:
            col_name = col[1]
            col_type = col[2] or "TEXT"
            col_descriptions.append(f"  {col_name} {col_type}")

        # Get row count
        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        # Get sample values for key columns
        sample_row = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 1').fetchone()

        schema_parts.append(
            f"TABLE {table_name} ({count} rows):\n" + "\n".join(col_descriptions)
        )

    return "\n\n".join(schema_parts)


def get_table_names() -> list[str]:
    """Get all data table names (excluding internal tables)."""
    conn = get_connection()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('edges', 'graph_nodes') ORDER BY name"
    ).fetchall()
    return [t[0] for t in tables]


def get_node_metadata(node_id: str, node_type: str) -> Optional[dict]:
    """
    Fetch full record for a graph node by looking up its source table.
    Handles composite IDs (e.g. 'val1::val2') by splitting on '::'.
    """
    conn = get_connection()

    # Map node_type to table + PK columns
    TABLE_PK_MAP = {
        "sales_order_headers": ("sales_order_headers", ["salesOrder"]),
        "sales_order_items": ("sales_order_items", ["salesOrder", "salesOrderItem"]),
        "outbound_delivery_headers": ("outbound_delivery_headers", ["deliveryDocument"]),
        "outbound_delivery_items": ("outbound_delivery_items", ["deliveryDocument", "deliveryDocumentItem"]),
        "billing_document_headers": ("billing_document_headers", ["billingDocument"]),
        "billing_document_items": ("billing_document_items", ["billingDocument", "billingDocumentItem"]),
        "billing_document_cancellations": ("billing_document_cancellations", ["billingDocument"]),
        "journal_entry_items": ("journal_entry_items", ["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"]),
        "payments": ("payments", ["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"]),
        "business_partners": ("business_partners", ["businessPartner"]),
        "products": ("products", ["product"]),
        "product_descriptions": ("product_descriptions", ["product", "language"]),
        "plants": ("plants", ["plant"]),
    }

    if node_type not in TABLE_PK_MAP:
        return None

    table_name, pk_cols = TABLE_PK_MAP[node_type]

    # Handle cancellation node IDs (CAN_billingDocument)
    actual_id = node_id
    if node_type == "billing_document_cancellations" and node_id.startswith("CAN_"):
        actual_id = node_id[4:]

    # Split composite ID
    id_parts = actual_id.split("::")
    if len(id_parts) != len(pk_cols):
        return None

    where_clauses = [f'"{col}" = ?' for col in pk_cols]
    where_sql = " AND ".join(where_clauses)

    try:
        row = conn.execute(
            f'SELECT * FROM "{table_name}" WHERE {where_sql}', tuple(id_parts)
        ).fetchone()
        if row:
            return dict(row)
    except Exception:
        pass
    return None
