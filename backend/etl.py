"""
ETL Script: Load SAP O2C JSONL data into SQLite + build graph edges table.

Usage:
    python etl.py [--data-dir ../sap-o2c-data] [--db-path ../o2c_data.db]
"""

import json
import os
import sqlite3
import argparse
from pathlib import Path


# ── Entity definitions ──────────────────────────────────────────────────────
# Each entity: folder_name -> (table_name, primary_key_columns)
ENTITIES = {
    "sales_order_headers": ("sales_order_headers", ["salesOrder"]),
    "sales_order_items": ("sales_order_items", ["salesOrder", "salesOrderItem"]),
    "sales_order_schedule_lines": ("sales_order_schedule_lines", ["salesOrder", "salesOrderItem", "scheduleLine"]),
    "outbound_delivery_headers": ("outbound_delivery_headers", ["deliveryDocument"]),
    "outbound_delivery_items": ("outbound_delivery_items", ["deliveryDocument", "deliveryDocumentItem"]),
    "billing_document_headers": ("billing_document_headers", ["billingDocument"]),
    "billing_document_items": ("billing_document_items", ["billingDocument", "billingDocumentItem"]),
    "billing_document_cancellations": ("billing_document_cancellations", ["billingDocument"]),
    "journal_entry_items_accounts_receivable": ("journal_entry_items", ["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"]),
    "payments_accounts_receivable": ("payments", ["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"]),
    "business_partners": ("business_partners", ["businessPartner"]),
    "business_partner_addresses": ("business_partner_addresses", ["businessPartner", "addressId"]),
    "customer_company_assignments": ("customer_company_assignments", ["customer", "companyCode"]),
    "customer_sales_area_assignments": ("customer_sales_area_assignments", ["customer", "salesOrganization", "distributionChannel", "division"]),
    "products": ("products", ["product"]),
    "product_descriptions": ("product_descriptions", ["product", "language"]),
    "product_plants": ("product_plants", ["product", "plant"]),
    "product_storage_locations": ("product_storage_locations", ["product", "plant", "storageLocation"]),
    "plants": ("plants", ["plant"]),
}

# Entities that are part of the core O2C graph (nodes + edges)
GRAPH_ENTITY_TYPES = {
    "sales_order_headers",
    "sales_order_items",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "journal_entry_items",
    "payments",
    "business_partners",
    "products",
    "product_descriptions",
    "plants",
}


def flatten_value(v):
    """Flatten nested JSON values (e.g. time objects) to string."""
    if isinstance(v, dict):
        return json.dumps(v)
    if isinstance(v, list):
        return json.dumps(v)
    return v


def read_jsonl_folder(folder_path: Path):
    """Read all JSONL files in a folder and yield rows as dicts."""
    for fpath in sorted(folder_path.glob("*.jsonl")):
        with open(fpath, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def infer_columns(rows: list) -> list:
    """Collect all unique column names across rows, preserving order."""
    seen = {}
    for row in rows:
        for key in row:
            if key not in seen:
                seen[key] = len(seen)
    return sorted(seen.keys(), key=lambda k: seen[k])


def create_table(conn: sqlite3.Connection, table_name: str, columns: list[str], pk_cols: list[str]):
    """Create a table with TEXT columns and a composite primary key."""
    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    pk_def = ", ".join(f'"{col}"' for col in pk_cols)
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs}, PRIMARY KEY ({pk_def}))'
    conn.execute(sql)


def insert_rows(conn: sqlite3.Connection, table_name: str, columns: list[str], rows: list[dict]):
    """Insert rows into a table with INSERT OR IGNORE for PK conflicts."""
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT OR IGNORE INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
    batch = []
    for row in rows:
        values = tuple(flatten_value(row.get(col)) for col in columns)
        batch.append(values)
        if len(batch) >= 500:
            conn.executemany(sql, batch)
            batch = []
    if batch:
        conn.executemany(sql, batch)


def create_edges_table(conn: sqlite3.Connection):
    """Create the graph edges table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            source_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            target_type TEXT NOT NULL,
            relationship TEXT NOT NULL,
            PRIMARY KEY (source_id, source_type, target_id, target_type, relationship)
        )
    """)


def make_node_id(table_name: str, pk_cols: list[str], row: dict) -> str:
    """Build a unique node ID from the table name and PK values."""
    parts = [str(row.get(col, "")) for col in pk_cols]
    return "::".join(parts) if len(parts) > 1 else parts[0]


def build_edges(conn: sqlite3.Connection):
    """
    Infer graph edges from foreign key relationships in the O2C flow.
    Each edge: (source_id, source_type, target_id, target_type, relationship)
    """
    edges = []

    # 1. business_partners → PLACED_ORDER → sales_order_headers
    rows = conn.execute("""
        SELECT bp.businessPartner, soh.salesOrder
        FROM business_partners bp
        JOIN sales_order_headers soh ON soh.soldToParty = bp.businessPartner
    """).fetchall()
    for bp_id, so_id in rows:
        edges.append((bp_id, "business_partners", so_id, "sales_order_headers", "PLACED_ORDER"))

    # 2. sales_order_headers → HAS_ITEM → sales_order_items
    rows = conn.execute("""
        SELECT soh.salesOrder, soi.salesOrder, soi.salesOrderItem
        FROM sales_order_headers soh
        JOIN sales_order_items soi ON soi.salesOrder = soh.salesOrder
    """).fetchall()
    for _, so_id, item_id in rows:
        edges.append((so_id, "sales_order_headers", f"{so_id}::{item_id}", "sales_order_items", "HAS_ITEM"))

    # 3. sales_order_items → FOR_PRODUCT → products
    rows = conn.execute("""
        SELECT soi.salesOrder, soi.salesOrderItem, soi.material
        FROM sales_order_items soi
        WHERE soi.material IN (SELECT product FROM products)
    """).fetchall()
    for so_id, item_id, product_id in rows:
        edges.append((f"{so_id}::{item_id}", "sales_order_items", product_id, "products", "FOR_PRODUCT"))

    # 4. outbound_delivery_items → DELIVERS_ORDER_ITEM → sales_order_items
    rows = conn.execute("""
        SELECT odi.deliveryDocument, odi.deliveryDocumentItem,
               odi.referenceSdDocument, odi.referenceSdDocumentItem
        FROM outbound_delivery_items odi
        WHERE odi.referenceSdDocument IS NOT NULL AND odi.referenceSdDocument != ''
    """).fetchall()
    for del_doc, del_item, ref_doc, ref_item in rows:
        # Strip leading zeros from ref_item for matching
        ref_item_clean = ref_item.lstrip("0") if ref_item else ref_item
        edges.append((
            f"{del_doc}::{del_item}", "outbound_delivery_items",
            f"{ref_doc}::{ref_item_clean}", "sales_order_items",
            "DELIVERS_ORDER_ITEM"
        ))

    # 5. outbound_delivery_headers → HAS_DELIVERY_ITEM → outbound_delivery_items
    rows = conn.execute("""
        SELECT odh.deliveryDocument, odi.deliveryDocument, odi.deliveryDocumentItem
        FROM outbound_delivery_headers odh
        JOIN outbound_delivery_items odi ON odi.deliveryDocument = odh.deliveryDocument
    """).fetchall()
    for _, del_doc, del_item in rows:
        edges.append((del_doc, "outbound_delivery_headers", f"{del_doc}::{del_item}", "outbound_delivery_items", "HAS_DELIVERY_ITEM"))

    # 6. billing_document_items → BILLS_DELIVERY → outbound_delivery_items
    rows = conn.execute("""
        SELECT bdi.billingDocument, bdi.billingDocumentItem,
               bdi.referenceSdDocument, bdi.referenceSdDocumentItem
        FROM billing_document_items bdi
        WHERE bdi.referenceSdDocument IS NOT NULL AND bdi.referenceSdDocument != ''
    """).fetchall()
    for bill_doc, bill_item, ref_doc, ref_item in rows:
        ref_item_clean = ref_item.lstrip("0") if ref_item else ref_item
        edges.append((
            f"{bill_doc}::{bill_item}", "billing_document_items",
            f"{ref_doc}::{ref_item_clean}", "outbound_delivery_items",
            "BILLS_DELIVERY"
        ))

    # 7. billing_document_headers → HAS_BILLING_ITEM → billing_document_items
    rows = conn.execute("""
        SELECT bdh.billingDocument, bdi.billingDocument, bdi.billingDocumentItem
        FROM billing_document_headers bdh
        JOIN billing_document_items bdi ON bdi.billingDocument = bdh.billingDocument
    """).fetchall()
    for _, bill_doc, bill_item in rows:
        edges.append((bill_doc, "billing_document_headers", f"{bill_doc}::{bill_item}", "billing_document_items", "HAS_BILLING_ITEM"))

    # 8. billing_document_headers → CREATES_JOURNAL → journal_entry_items
    rows = conn.execute("""
        SELECT bdh.billingDocument, bdh.accountingDocument, jei.companyCode, jei.fiscalYear, jei.accountingDocumentItem
        FROM billing_document_headers bdh
        JOIN journal_entry_items jei
          ON jei.accountingDocument = bdh.accountingDocument
    """).fetchall()
    for bill_doc, acc_doc, company, fy, acc_item in rows:
        edges.append((
            bill_doc, "billing_document_headers",
            f"{company}::{fy}::{acc_doc}::{acc_item}", "journal_entry_items",
            "CREATES_JOURNAL"
        ))

    # 9. billing_document_headers → SOLD_TO → business_partners
    rows = conn.execute("""
        SELECT bdh.billingDocument, bdh.soldToParty
        FROM billing_document_headers bdh
        WHERE bdh.soldToParty IN (SELECT businessPartner FROM business_partners)
    """).fetchall()
    for bill_doc, bp_id in rows:
        edges.append((bill_doc, "billing_document_headers", bp_id, "business_partners", "SOLD_TO"))

    # 10. billing_document_cancellations → CANCELS → billing_document_headers
    rows = conn.execute("""
        SELECT bdc.billingDocument
        FROM billing_document_cancellations bdc
        WHERE bdc.billingDocument IN (SELECT billingDocument FROM billing_document_headers)
    """).fetchall()
    for (bill_doc,) in rows:
        edges.append((f"CAN_{bill_doc}", "billing_document_cancellations", bill_doc, "billing_document_headers", "CANCELS"))

    # 11. journal_entry_items → CLEARED_BY → payments
    rows = conn.execute("""
        SELECT jei.companyCode, jei.fiscalYear, jei.accountingDocument, jei.accountingDocumentItem,
               p.companyCode, p.fiscalYear, p.accountingDocument, p.accountingDocumentItem
        FROM journal_entry_items jei
        JOIN payments p
          ON p.accountingDocument = jei.clearingAccountingDocument
          AND p.companyCode = jei.companyCode
          AND p.fiscalYear = COALESCE(jei.clearingDocFiscalYear, jei.fiscalYear)
        WHERE jei.clearingAccountingDocument IS NOT NULL AND jei.clearingAccountingDocument != ''
    """).fetchall()
    for j_cc, j_fy, j_ad, j_ai, p_cc, p_fy, p_ad, p_ai in rows:
        edges.append((
            f"{j_cc}::{j_fy}::{j_ad}::{j_ai}", "journal_entry_items",
            f"{p_cc}::{p_fy}::{p_ad}::{p_ai}", "payments",
            "CLEARED_BY"
        ))

    # 12. products → DESCRIBED_AS → product_descriptions
    rows = conn.execute("""
        SELECT p.product, pd.product, pd.language
        FROM products p
        JOIN product_descriptions pd ON pd.product = p.product
    """).fetchall()
    for _, prod_id, lang in rows:
        edges.append((prod_id, "products", f"{prod_id}::{lang}", "product_descriptions", "DESCRIBED_AS"))

    # Insert all edges
    conn.executemany(
        "INSERT OR IGNORE INTO edges (source_id, source_type, target_id, target_type, relationship) VALUES (?, ?, ?, ?, ?)",
        edges
    )
    return len(edges)


def build_graph_nodes_table(conn: sqlite3.Connection):
    """
    Create a graph_nodes table with (node_id, node_type, label) for fast graph serving.
    Only includes core O2C entity types.
    """
    conn.execute("DROP TABLE IF EXISTS graph_nodes")
    conn.execute("""
        CREATE TABLE graph_nodes (
            node_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            label TEXT,
            PRIMARY KEY (node_id, node_type)
        )
    """)

    # Sales Order Headers
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT salesOrder, 'sales_order_headers', 'SO-' || salesOrder
        FROM sales_order_headers
    """)

    # Sales Order Items
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT salesOrder || '::' || salesOrderItem, 'sales_order_items',
               'SOI-' || salesOrder || '/' || salesOrderItem
        FROM sales_order_items
    """)

    # Outbound Delivery Headers
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT deliveryDocument, 'outbound_delivery_headers', 'DEL-' || deliveryDocument
        FROM outbound_delivery_headers
    """)

    # Outbound Delivery Items
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT deliveryDocument || '::' || deliveryDocumentItem, 'outbound_delivery_items',
               'DI-' || deliveryDocument || '/' || deliveryDocumentItem
        FROM outbound_delivery_items
    """)

    # Billing Document Headers
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT billingDocument, 'billing_document_headers', 'BILL-' || billingDocument
        FROM billing_document_headers
    """)

    # Billing Document Items
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT billingDocument || '::' || billingDocumentItem, 'billing_document_items',
               'BI-' || billingDocument || '/' || billingDocumentItem
        FROM billing_document_items
    """)

    # Billing Document Cancellations
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT 'CAN_' || billingDocument, 'billing_document_cancellations', 'CANCEL-' || billingDocument
        FROM billing_document_cancellations
    """)

    # Journal Entry Items
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT companyCode || '::' || fiscalYear || '::' || accountingDocument || '::' || accountingDocumentItem,
               'journal_entry_items',
               'JE-' || accountingDocument || '/' || accountingDocumentItem
        FROM journal_entry_items
    """)

    # Payments
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT companyCode || '::' || fiscalYear || '::' || accountingDocument || '::' || accountingDocumentItem,
               'payments',
               'PAY-' || accountingDocument || '/' || accountingDocumentItem
        FROM payments
    """)

    # Business Partners
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT businessPartner, 'business_partners', 
               COALESCE(businessPartnerName, 'BP-' || businessPartner)
        FROM business_partners
    """)

    # Products
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT p.product, 'products',
               COALESCE(pd.productDescription, 'PROD-' || p.product)
        FROM products p
        LEFT JOIN product_descriptions pd ON pd.product = p.product AND pd.language = 'EN'
    """)

    # Product Descriptions
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT product || '::' || language, 'product_descriptions',
               productDescription
        FROM product_descriptions
    """)

    # Plants
    conn.execute("""
        INSERT OR IGNORE INTO graph_nodes (node_id, node_type, label)
        SELECT plant, 'plants', COALESCE(plantName, 'PLANT-' || plant)
        FROM plants
    """)

    count = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    return count


def main():
    abs_file = os.path.abspath(__file__)
    # Correct BASE_DIR is the root folder containing both backend/ and sap-o2c-data/
    root = Path(abs_file).resolve().parent.parent
    
    DEFAULT_DATA_DIR = os.getenv("DATA_DIR", str(root / "sap-o2c-data"))
    DEFAULT_DB_PATH = os.getenv("DB_PATH", str(root / "o2c_data.db"))

    parser = argparse.ArgumentParser(description="ETL: SAP O2C JSONL → SQLite")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Path to JSONL data directory")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Output SQLite database path")
    args = parser.parse_args()

    data_dir = Path(args.data_dir) # Use raw path from default/arg
    db_path = Path(args.db_path)

    if db_path.exists():
        os.remove(db_path)

    print(f"DEBUG: __file__ is {abs_file}")
    print(f"DEBUG: Resolved PROJECT ROOT is {root}")
    print(f"DEBUG: Checking for data folder in root: {root / 'sap-o2c-data'} -> {(root / 'sap-o2c-data').exists()}")

    # Prioritize the internal repository path if it exists
    repo_data_dir = root / "sap-o2c-data"
    if repo_data_dir.exists():
        data_dir = repo_data_dir
        print(f"💡 Automatically using repository data folder at {data_dir}")
    else:
        # Fallback to args/env
        data_dir = Path(args.data_dir)
        if not data_dir.is_absolute():
            data_dir = (root / data_dir).resolve()
        else:
            data_dir = data_dir.resolve()

    print(f"📂 Final resolved Data directory: {data_dir}")
    print(f"💾 Final resolved Database: {db_path}\n")
    flag_file = Path(__file__).parent / ".etl_initialized" # Track state

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # ── Load entities ──
    loaded_any = False
    for folder_name, (table_name, pk_cols) in ENTITIES.items():
        folder_path = data_dir / folder_name
        if not folder_path.exists():
            print(f"⚠ Skipping {folder_name} (folder not found at {folder_path})")
            continue

        rows = list(read_jsonl_folder(folder_path))
        if not rows:
            print(f"⚠ Skipping {folder_name} (no data)")
            continue

        columns = infer_columns(rows)
        create_table(conn, table_name, columns, pk_cols)
        insert_rows(conn, table_name, columns, rows)
        conn.commit()
        print(f"✅ {table_name}: {len(rows)} rows, {len(columns)} columns")
        loaded_any = True

    if not loaded_any:
        print("\n❌ Failed: No data folders found. Please ensure the data files are committed to Git.")
        conn.close()
        if db_path.exists():
            os.remove(db_path)
        return
    flagship_node_id = "business_partners::1" # Example

    # ── Build edges ──
    print("\n🔗 Building graph edges...")
    create_edges_table(conn)
    edge_count = build_edges(conn)
    conn.commit()
    print(f"✅ edges: {edge_count} relationships")

    # ── Build graph_nodes lookup ──
    print("\n🪢 Building graph_nodes table...")
    node_count = build_graph_nodes_table(conn)
    conn.commit()
    print(f"✅ graph_nodes: {node_count} nodes")

    # ── Summary ──
    print("\n📊 Database summary:")
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    for (tname,) in tables:
        count = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
        print(f"   {tname}: {count} rows")

    conn.close()
    print(f"\n✨ ETL complete! Database at: {db_path}")


if __name__ == "__main__":
    main()
