"""
LLM Service: Gemini-powered natural language query pipeline.

Three-stage pipeline:
1. Guardrail — keyword pre-filter + LLM classification
2. SQL Generation — NL → SQLite SQL
3. Narration — SQL results → human-friendly answer
"""

import os
import re
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

import google.generativeai as genai
from backend.database import get_schema_description, execute_query, get_table_names

# ── Configure Gemini ────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel("gemini-2.0-flash")

# ── Guardrail ───────────────────────────────────────────────────────────────

# Obvious off-topic keywords → instant reject
OFF_TOPIC_PATTERNS = [
    r"\b(poem|poetry|song|story|joke|recipe|weather|news)\b",
    r"\b(write me|compose|create a|tell me about|explain the history)\b",
    r"\b(translate|define|who is|what is the meaning)\b",
    r"\b(code|programming|python|javascript|html)\b",
]

DOMAIN_KEYWORDS = [
    "order", "sales", "delivery", "billing", "invoice", "payment",
    "customer", "product", "journal", "accounting", "amount", "quantity",
    "material", "plant", "business partner", "document", "cancelled",
    "net amount", "total", "currency", "status", "date", "created",
    "cleared", "shipped", "incoterms", "schedule", "fiscal",
]


def is_off_topic_by_keywords(message: str) -> bool:
    """Quick keyword-based check for obviously off-topic queries."""
    lower = message.lower()

    # Check if it matches any off-topic pattern
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, lower):
            # But also check if it contains domain keywords (could be dual-intent)
            for kw in DOMAIN_KEYWORDS:
                if kw in lower:
                    return False
            return True

    return False


async def llm_guardrail(message: str) -> bool:
    """
    LLM-based guardrail for borderline queries.
    Returns True if the query is ON-TOPIC (should proceed).
    """
    prompt = f"""Is the following question related to analyzing business data 
(sales orders, deliveries, billing documents, invoices, payments, customers, 
products, journal entries, accounting documents)?

Answer ONLY with YES or NO.

Question: {message}"""

    try:
        response = model.generate_content(prompt)
        answer = response.text.strip().upper()
        return answer.startswith("YES")
    except Exception:
        # On LLM failure, allow the query (fail open for guardrail)
        return True


async def check_guardrail(message: str) -> tuple[bool, str]:
    """
    Full guardrail check: keyword + LLM.
    Returns (is_allowed, rejection_reason).
    """
    if is_off_topic_by_keywords(message):
        return False, "This question doesn't appear to be related to the business dataset. I can help with questions about sales orders, deliveries, billing, payments, customers, and products."

    # For very short queries that might be ambiguous, use LLM
    if len(message.split()) <= 3:
        is_on_topic = await llm_guardrail(message)
        if not is_on_topic:
            return False, "I can only answer questions related to the Order-to-Cash business data. Try asking about sales orders, deliveries, billing documents, or payments."

    return True, ""


# ── SQL Generation ──────────────────────────────────────────────────────────

def _get_sql_system_prompt() -> str:
    schema = get_schema_description()
    return f"""You are a SQL assistant for a SAP Order-to-Cash business dataset stored in SQLite.
Generate ONLY a valid SQLite SQL query. No explanation, no markdown, no backticks, just raw SQL.

SCHEMA:
{schema}

IMPORTANT RELATIONSHIPS (use these for JOINs):
- sales_order_headers.soldToParty = business_partners.businessPartner (customer who placed the order)
- sales_order_items.salesOrder = sales_order_headers.salesOrder (items in an order)
- sales_order_items.material = products.product (product ordered)
- outbound_delivery_items.referenceSdDocument = sales_order_headers.salesOrder (delivery for an order)  
- outbound_delivery_items.deliveryDocument = outbound_delivery_headers.deliveryDocument
- billing_document_items.referenceSdDocument = outbound_delivery_headers.deliveryDocument (bill for a delivery)
- billing_document_items.billingDocument = billing_document_headers.billingDocument
- billing_document_headers.accountingDocument = journal_entry_items.accountingDocument 
- billing_document_headers.soldToParty = business_partners.businessPartner
- journal_entry_items.clearingAccountingDocument = payments.accountingDocument (payment clearing a journal entry)
- product_descriptions.product = products.product AND product_descriptions.language = 'EN'

RULES:
- Only use tables listed in the schema above
- Return at most 100 rows unless the user asks otherwise  
- Always alias columns clearly for readability
- NEVER use DROP, INSERT, UPDATE, DELETE, ALTER, or any DDL/DML
- Use LIKE with % for partial text matches
- Amounts are stored as TEXT, cast to REAL for calculations: CAST(column AS REAL)
- Dates are ISO format TEXT: use date() or substr() for date filtering
- For counting, use COUNT(*) or COUNT(DISTINCT ...)
- When grouping, include all non-aggregated columns in GROUP BY
"""


async def generate_sql(question: str, history: list[dict] = None) -> str:
    """Generate SQL from a natural language question."""
    system_prompt = _get_sql_system_prompt()

    messages = [{"role": "user", "parts": [system_prompt]}]
    messages.append({"role": "model", "parts": ["I understand. I will generate only valid SQLite SQL queries based on the schema provided."]})

    # Add conversation history for context
    if history:
        for entry in history[-5:]:  # Last 5 exchanges
            if entry.get("question"):
                messages.append({"role": "user", "parts": [f"Question: {entry['question']}"]})
            if entry.get("sql"):
                messages.append({"role": "model", "parts": [entry["sql"]]})

    messages.append({"role": "user", "parts": [f"Question: {question}\nSQL:"]})

    response = model.generate_content(messages)
    sql = response.text.strip()

    # Clean up: remove markdown code fences if present
    sql = re.sub(r"^```(?:sql)?\s*", "", sql)
    sql = re.sub(r"\s*```$", "", sql)
    sql = sql.strip()

    # Safety check: ensure it's a SELECT
    if not sql.upper().startswith("SELECT"):
        raise ValueError(f"Generated query is not a SELECT statement: {sql[:50]}")

    # Check for dangerous operations
    dangerous = re.search(r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE)\b", sql, re.IGNORECASE)
    if dangerous:
        raise ValueError(f"Generated query contains forbidden operation: {dangerous.group()}")

    return sql


async def execute_generated_sql(sql: str, retry_question: str = None) -> tuple[list[dict], str]:
    """
    Execute generated SQL with error handling and retry.
    Returns (results, final_sql).
    """
    try:
        results = execute_query(sql)
        return results, sql
    except Exception as e:
        if retry_question:
            # Retry: include the error in the prompt
            retry_prompt = f"""The previous SQL query failed with error: {str(e)}

Please fix the query. Original question: {retry_question}
Failed SQL: {sql}

Generate only the corrected SQL:"""
            try:
                response = model.generate_content(retry_prompt)
                fixed_sql = response.text.strip()
                fixed_sql = re.sub(r"^```(?:sql)?\s*", "", fixed_sql)
                fixed_sql = re.sub(r"\s*```$", "", fixed_sql)
                fixed_sql = fixed_sql.strip()
                results = execute_query(fixed_sql)
                return results, fixed_sql
            except Exception as retry_err:
                raise RuntimeError(f"SQL failed after retry: {retry_err}")
        raise


# ── Narration ───────────────────────────────────────────────────────────────

async def narrate_results(question: str, sql: str, results: list[dict]) -> str:
    """Generate a human-friendly narration of the SQL results."""
    if not results:
        return "No matching records were found for your query."

    # Truncate results for the prompt if too many
    display_results = results[:20]
    results_text = json.dumps(display_results, indent=2, default=str)
    total_count = len(results)

    prompt = f"""Given this business data query and its results, provide a clear, concise answer.

Question: {question}
SQL used: {sql}
Results ({total_count} rows total, showing first {len(display_results)}):
{results_text}

Instructions:
- Answer the question directly and concisely
- Include relevant numbers and specifics from the data
- If there are many results, summarize the key findings
- Use business-friendly language
- Do NOT include SQL in your response
- Keep the answer to 2-4 sentences maximum"""

    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        # Fallback: simple summary
        if total_count == 1 and len(results[0]) == 1:
            key = list(results[0].keys())[0]
            return f"The result is: {results[0][key]}"
        return f"Found {total_count} matching records."


# ── Entity ID Extraction ───────────────────────────────────────────────────

def extract_node_ids(results: list[dict]) -> list[str]:
    """
    Extract entity IDs from SQL results that can be highlighted on the graph.
    Looks for known ID columns and maps them to graph node IDs.
    """
    id_columns = {
        "salesOrder": "sales_order_headers",
        "billingDocument": "billing_document_headers",
        "deliveryDocument": "outbound_delivery_headers",
        "accountingDocument": "journal_entry_items",
        "businessPartner": "business_partners",
        "product": "products",
        "customer": "business_partners",
        "soldToParty": "business_partners",
        "material": "products",
    }

    node_ids = set()
    for row in results[:50]:  # Limit to avoid huge highlight sets
        for col, _node_type in id_columns.items():
            if col in row and row[col]:
                node_ids.add(str(row[col]))

    return list(node_ids)


# ── Full Chat Pipeline ──────────────────────────────────────────────────────

async def chat(message: str, history: list[dict] = None) -> dict:
    """
    Full chat pipeline:
    1. Guardrail check
    2. Generate SQL
    3. Execute SQL (with retry)
    4. Narrate results
    5. Extract node IDs for graph highlighting
    """
    # 1. Guardrail
    is_allowed, rejection_reason = await check_guardrail(message)
    if not is_allowed:
        return {
            "answer": rejection_reason,
            "sql": None,
            "data": [],
            "nodes": [],
            "rejected": True,
        }

    try:
        # 2. Generate SQL
        sql = await generate_sql(message, history)

        # 3. Execute
        results, final_sql = await execute_generated_sql(sql, retry_question=message)

        # 4. Narrate
        answer = await narrate_results(message, final_sql, results)

        # 5. Extract node IDs
        nodes = extract_node_ids(results)

        return {
            "answer": answer,
            "sql": final_sql,
            "data": results[:100],  # Cap data sent to frontend
            "nodes": nodes,
            "rejected": False,
        }

    except Exception as e:
        return {
            "answer": f"I encountered an error processing your query: {str(e)}. Please try rephrasing your question.",
            "sql": None,
            "data": [],
            "nodes": [],
            "rejected": False,
            "error": str(e),
        }
