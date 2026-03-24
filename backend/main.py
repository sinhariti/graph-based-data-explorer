"""
FastAPI main application: Graph-based Data Explorer API.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from backend.database import get_schema_description, get_node_metadata
from backend.graph_service import (
    get_all_nodes,
    get_all_edges,
    get_neighbors,
    get_subgraph_for_ids,
    get_graph_stats,
)
from backend.llm_service import chat as llm_chat

# ── App setup ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="O2C Graph Explorer API",
    description="Graph-based SAP Order-to-Cash data explorer with NL querying",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return {
        "error": str(exc),
        "detail": "Internal Server Error",
        "type": type(exc).__name__
    }


# ── Request/Response models ─────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: Optional[list[dict]] = None


class ChatResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    data: list[dict] = []
    nodes: list[str] = []
    rejected: bool = False
    error: Optional[str] = None


# ── Graph endpoints ─────────────────────────────────────────────────────────

@app.get("/api/graph/nodes")
async def graph_nodes():
    """Return all graph nodes."""
    nodes = get_all_nodes()
    return {"nodes": nodes, "count": len(nodes)}


@app.get("/api/graph/edges")
async def graph_edges():
    """Return all graph edges."""
    edges = get_all_edges()
    return {"edges": edges, "count": len(edges)}


@app.get("/api/graph/full")
async def graph_full():
    """Return the full graph (nodes + edges) in Cytoscape.js format."""
    nodes = get_all_nodes()
    edges = get_all_edges()

    elements = []
    # Nodes as Cytoscape elements
    for n in nodes:
        elements.append({
            "group": "nodes",
            "data": {
                "id": n["id"],
                "label": n["label"],
                "type": n["type"],
            }
        })
    # Edges as Cytoscape elements
    for e in edges:
        elements.append({
            "group": "edges",
            "data": {
                "id": f"{e['source']}->{e['target']}",
                "source": e["source"],
                "target": e["target"],
                "relationship": e["relationship"],
            }
        })

    return {"elements": elements, "nodeCount": len(nodes), "edgeCount": len(edges)}


@app.get("/api/graph/expand/{node_id:path}")
async def graph_expand(node_id: str):
    """Return neighbors of a node for expand-on-click."""
    result = get_neighbors(node_id)
    if not result["nodes"]:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    # Convert to Cytoscape elements format
    elements = []
    for n in result["nodes"]:
        elements.append({
            "group": "nodes",
            "data": {"id": n["id"], "label": n["label"], "type": n["type"]}
        })
    for e in result["edges"]:
        elements.append({
            "group": "edges",
            "data": {
                "id": f"{e['source']}->{e['target']}",
                "source": e["source"],
                "target": e["target"],
                "relationship": e["relationship"],
            }
        })

    return {"elements": elements, "nodeCount": len(result["nodes"]), "edgeCount": len(result["edges"])}


@app.get("/api/graph/node/{node_id:path}")
async def graph_node_detail(node_id: str, node_type: str = ""):
    """Return full metadata for a specific node."""
    if not node_type:
        # Try to infer type from graph_nodes table
        from backend.database import execute_query
        rows = execute_query("SELECT node_type FROM graph_nodes WHERE node_id = ?", (node_id,))
        if rows:
            node_type = rows[0]["node_type"]
        else:
            raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    metadata = get_node_metadata(node_id, node_type)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Metadata not found for {node_type}/{node_id}")

    # Count connections
    neighbors = get_neighbors(node_id)
    connection_count = len(neighbors["nodes"])

    return {
        "node_id": node_id,
        "node_type": node_type,
        "metadata": metadata,
        "connections": connection_count,
    }


@app.get("/api/graph/stats")
async def graph_stats():
    """Return graph statistics."""
    return get_graph_stats()


# ── Chat endpoint ───────────────────────────────────────────────────────────

@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """Natural language query → SQL → narrated answer + highlighted nodes."""
    result = await llm_chat(request.message, request.history)
    return ChatResponse(**result)


# ── Schema endpoint (debugging) ────────────────────────────────────────────

@app.get("/api/schema")
async def schema():
    """Return the database schema description (for debugging)."""
    return {"schema": get_schema_description()}


# ── Health check ────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    """Health check endpoint."""
    stats = get_graph_stats()
    return {
        "status": "healthy",
        "graph_nodes": stats["total_nodes"],
        "graph_edges": stats["total_edges"],
    }
