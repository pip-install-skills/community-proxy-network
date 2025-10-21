import uuid
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlmodel import SQLModel, Session, create_engine, select
from models import Node, MatchRequest
from datetime import datetime
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./nodes.db")
engine = create_engine(DATABASE_URL, echo=False)
app = FastAPI(title="Community Proxy Control Plane")

# In-memory ws connections for simple PoC
WS_CONNECTIONS = {}

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)

@app.post("/register")
async def register(node_id: str, public_key: str = None, ip: str = None, country: str = None):
    with Session(engine) as session:
        existing = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if existing:
            existing.public_key = public_key or existing.public_key
            existing.ip = ip or existing.ip
            existing.country = country or existing.country
            existing.last_seen = datetime.utcnow()
            session.add(existing)
            session.commit()
            return {"status": "ok", "node_id": node_id}

        node = Node(node_id=node_id, public_key=public_key, ip=ip, country=country)
        session.add(node)
        session.commit()
        return {"status": "registered", "node_id": node_id}

@app.post("/heartbeat")
async def heartbeat(node_id: str):
    with Session(engine) as session:
        node = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if not node:
            raise HTTPException(status_code=404, detail="node not found")
        node.last_seen = datetime.utcnow()
        session.add(node)
        session.commit()
        return {"status": "ok"}

@app.get("/match")
async def match(credits_required: int = 0, country: str = None):
    # very naive match: pick first non-banned node with credits>=0
    with Session(engine) as session:
        q = select(Node).where(Node.banned == False)
        if country:
            q = q.where(Node.country == country)
        node = session.exec(q).first()
        if not node:
            raise HTTPException(status_code=404, detail="no node available")
        return {"node_id": node.node_id, "ip": node.ip, "country": node.country}

@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONNECTIONS[node_id] = websocket
    try:
        while True:
            data = await websocket.receive_text()
            # For PoC, just echo back commands
            print(f"WS from {node_id}: {data}")
            await websocket.send_text(f"ack: {data}")
    except WebSocketDisconnect:
        WS_CONNECTIONS.pop(node_id, None)
        print(f"WS disconnected: {node_id}")

@app.post("/send-command/{node_id}")
async def send_command(node_id: str, cmd: str):
    ws = WS_CONNECTIONS.get(node_id)
    if not ws:
        raise HTTPException(status_code=404, detail="node not connected")
    await ws.send_text(cmd)
    return {"status": "sent"}