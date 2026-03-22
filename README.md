# Graph-Based Data Explorer — Order to Cash

An interactive graph-based explorer for SAP Order-to-Cash business data. Visualize entity relationships with Cytoscape.js and query the data using natural language powered by Gemini AI.

## Quick Start

### 1. Backend

```bash
cd backend
pip3 install -r requirements.txt

# Run ETL (loads JSONL data → SQLite + graph)
python3 etl.py

# Add your Gemini API key
echo "GEMINI_API_KEY=your_key_here" > .env

# Start the API server
cd .. && python3 -m uvicorn backend.main:app --port 8000
```

### 2. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser.

## Architecture

```
├── sap-o2c-data/          # Raw JSONL dataset (19 entity types)
├── backend/
│   ├── etl.py             # JSONL → SQLite + graph edges
│   ├── database.py        # SQLite connection + schema introspection
│   ├── graph_service.py   # NetworkX graph traversal
│   ├── llm_service.py     # Gemini: guardrail → SQL → narration
│   └── main.py            # FastAPI endpoints
├── frontend/
│   └── src/
│       ├── App.jsx            # Main orchestration
│       └── components/
│           ├── GraphView.jsx  # Cytoscape.js visualization
│           ├── ChatPanel.jsx  # NL query chat
│           └── NodeDetail.jsx # Entity metadata card
└── o2c_data.db            # Generated SQLite database
```

## Features

- **Graph Visualization**: 1,400+ nodes, 1,700+ edges, 13 entity types with color-coded nodes
- **Natural Language Queries**: Ask questions about orders, deliveries, billing, payments
- **SQL Transparency**: See the generated SQL behind every answer
- **Node Inspector**: Click any node to see full metadata
- **Guardrails**: Off-topic queries are rejected gracefully

## Tech Stack

| Layer | Tech |
|---|---|
| Database | SQLite + NetworkX |  
| Backend | FastAPI (Python) |
| LLM | Gemini 2.0 Flash |
| Frontend | React + Vite |
| Graph Viz | Cytoscape.js + dagre |
