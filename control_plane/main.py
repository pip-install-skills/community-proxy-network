from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlmodel import SQLModel, Session, create_engine, select
from models import Node
import asyncio
import json
import os
import random

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./nodes.db")
engine = create_engine(DATABASE_URL, echo=False)
app = FastAPI(title="Community Proxy Control Plane")

WS_CONNECTIONS = {}  # active node management sockets
RELAY_CONNECTIONS = {}  # for /ws/relay nodes

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)

# Node registration
@app.post("/register")
async def register(node_id: str, public_key: str = None, ip: str = None, country: str = None):
    with Session(engine) as session:
        existing = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if existing:
            existing.ip = ip or existing.ip
            existing.country = country or existing.country
            existing.last_seen = datetime.now(timezone.utc)
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

@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONNECTIONS[node_id] = websocket
    try:
        while True:
            msg = await websocket.receive_text()
            print(f"Mgmt WS from {node_id}: {msg}")
    except WebSocketDisconnect:
        WS_CONNECTIONS.pop(node_id, None)
        print(f"Mgmt WS disconnected: {node_id}")

@app.websocket("/ws/relay/{node_id}")
async def ws_relay(websocket: WebSocket, node_id: str):
    """ WebSocket channel used for relaying proxy data through this node """
    await websocket.accept()
    RELAY_CONNECTIONS[node_id] = websocket
    print(f"Relay node connected: {node_id}")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        RELAY_CONNECTIONS.pop(node_id, None)
        print(f"Relay node disconnected: {node_id}")

@app.websocket("/ws/proxy/{node_id}")
async def ws_proxy(websocket: WebSocket, node_id: str):
    """Originating agent sends SOCKS traffic here"""
    await websocket.accept()
    print(f"Proxy request from {node_id}")

    try:
        msg = await websocket.receive_text()
        cmd = json.loads(msg)
        if cmd.get("cmd") != "connect":
            await websocket.send_text(json.dumps({"error": "first message must be connect"}))
            await websocket.close()
            return

        target_host = cmd["host"]
        target_port = int(cmd.get("port", 80))

        # Choose an exit node (not self)
        exit_nodes = [n for n in RELAY_CONNECTIONS.keys() if n != node_id]
        if not exit_nodes:
            await websocket.send_text(json.dumps({"error": "no exit nodes available"}))
            await websocket.close()
            return

        chosen_node = random.choice(exit_nodes)
        exit_ws = RELAY_CONNECTIONS[chosen_node]
        print(f"Routing traffic via exit node {chosen_node}")

        # Ask exit node to connect
        await exit_ws.send_text(json.dumps({"cmd": "connect", "host": target_host, "port": target_port}))
        await websocket.send_text(json.dumps({"status": "connected", "exit": chosen_node}))

        # Bidirectional relay between origin and exit nodes
        async def origin_to_exit():
            try:
                while True:
                    data = await websocket.receive_bytes()
                    await exit_ws.send_bytes(data)
            except Exception:
                pass
            try:
                await exit_ws.close()
            except:
                pass

        async def exit_to_origin():
            try:
                while True:
                    data = await exit_ws.receive_bytes()
                    await websocket.send_bytes(data)
            except Exception:
                pass
            try:
                await websocket.close()
            except:
                pass

        await asyncio.gather(origin_to_exit(), exit_to_origin())

    except Exception as e:
        print(f"Proxy WS error: {e}")
        try:
            await websocket.send_text(json.dumps({"error": str(e)}))
        except:
            pass
        await websocket.close()
