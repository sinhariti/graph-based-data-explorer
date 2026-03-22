"""
Graph service: NetworkX graph loaded from SQLite edges table.
Provides node/edge data for the frontend Cytoscape.js visualization.
"""

from typing import Optional
import networkx as nx
from backend.database import get_connection


_graph: Optional[nx.DiGraph] = None


def _load_graph() -> nx.DiGraph:
    """Load the graph from the edges and graph_nodes tables into NetworkX."""
    conn = get_connection()
    G = nx.DiGraph()

    # Load nodes
    nodes = conn.execute("SELECT node_id, node_type, label FROM graph_nodes").fetchall()
    for node_id, node_type, label in nodes:
        G.add_node(node_id, node_type=node_type, label=label or node_id)

    # Load edges
    edges = conn.execute(
        "SELECT source_id, source_type, target_id, target_type, relationship FROM edges"
    ).fetchall()
    for src_id, src_type, tgt_id, tgt_type, rel in edges:
        # Ensure both endpoints exist as nodes
        if not G.has_node(src_id):
            G.add_node(src_id, node_type=src_type, label=src_id)
        if not G.has_node(tgt_id):
            G.add_node(tgt_id, node_type=tgt_type, label=tgt_id)
        G.add_edge(src_id, tgt_id, relationship=rel)

    return G


def get_graph() -> nx.DiGraph:
    """Get or create the singleton NetworkX graph."""
    global _graph
    if _graph is None:
        _graph = _load_graph()
    return _graph


def reload_graph():
    """Force reload the graph (e.g., after ETL)."""
    global _graph
    _graph = _load_graph()


def get_all_nodes() -> list[dict]:
    """Return all graph nodes as dicts for the frontend."""
    G = get_graph()
    result = []
    for node_id, attrs in G.nodes(data=True):
        result.append({
            "id": node_id,
            "type": attrs.get("node_type", "unknown"),
            "label": attrs.get("label", node_id),
        })
    return result


def get_all_edges() -> list[dict]:
    """Return all graph edges as dicts for the frontend."""
    G = get_graph()
    result = []
    for src, tgt, attrs in G.edges(data=True):
        result.append({
            "source": src,
            "target": tgt,
            "relationship": attrs.get("relationship", ""),
        })
    return result


def get_neighbors(node_id: str) -> dict:
    """
    Get 1-hop neighbors of a node (both predecessors and successors).
    Returns nodes and edges for the subgraph.
    """
    G = get_graph()
    if not G.has_node(node_id):
        return {"nodes": [], "edges": []}

    neighbor_ids = set()
    edges = []

    # Successors (outgoing edges)
    for succ in G.successors(node_id):
        neighbor_ids.add(succ)
        edge_data = G.edges[node_id, succ]
        edges.append({
            "source": node_id,
            "target": succ,
            "relationship": edge_data.get("relationship", ""),
        })

    # Predecessors (incoming edges)
    for pred in G.predecessors(node_id):
        neighbor_ids.add(pred)
        edge_data = G.edges[pred, node_id]
        edges.append({
            "source": pred,
            "target": node_id,
            "relationship": edge_data.get("relationship", ""),
        })

    # Build neighbor node list
    nodes = []
    for nid in neighbor_ids:
        attrs = G.nodes[nid]
        nodes.append({
            "id": nid,
            "type": attrs.get("node_type", "unknown"),
            "label": attrs.get("label", nid),
        })

    return {"nodes": nodes, "edges": edges}


def get_subgraph_for_ids(node_ids: list[str]) -> dict:
    """
    Given a list of node IDs (from chat results), return the subgraph
    connecting them (the nodes themselves + edges between them).
    """
    G = get_graph()
    existing = [nid for nid in node_ids if G.has_node(nid)]

    nodes = []
    for nid in existing:
        attrs = G.nodes[nid]
        nodes.append({
            "id": nid,
            "type": attrs.get("node_type", "unknown"),
            "label": attrs.get("label", nid),
        })

    edges = []
    for i, src in enumerate(existing):
        for tgt in existing[i + 1:]:
            if G.has_edge(src, tgt):
                edge_data = G.edges[src, tgt]
                edges.append({
                    "source": src,
                    "target": tgt,
                    "relationship": edge_data.get("relationship", ""),
                })
            if G.has_edge(tgt, src):
                edge_data = G.edges[tgt, src]
                edges.append({
                    "source": tgt,
                    "target": src,
                    "relationship": edge_data.get("relationship", ""),
                })

    return {"nodes": nodes, "edges": edges}


def get_graph_stats() -> dict:
    """Return basic graph statistics."""
    G = get_graph()
    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "node_types": dict(
            sorted(
                {
                    t: len([n for n, d in G.nodes(data=True) if d.get("node_type") == t])
                    for t in set(nx.get_node_attributes(G, "node_type").values())
                }.items()
            )
        ),
        "is_connected": nx.is_weakly_connected(G) if G.number_of_nodes() > 0 else False,
        "connected_components": nx.number_weakly_connected_components(G),
    }
