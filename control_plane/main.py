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

# In-memory ws connections
WS_CONNECTIONS = {}

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)

# Node registration
@app.post("/register")
async def register(node_id: str, public_key: str = None, ip: str = None, country: str = None):
    with Session(engine) as session:
        existing = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if existing:
            existing.public_key = public_key or existing.public_key
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

# Heartbeat
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

# Match endpoint (naive)
@app.get("/match")
async def match(credits_required: int = 0, country: str = None):
    with Session(engine) as session:
        q = select(Node).where(Node.banned == False)
        if country:
            q = q.where(Node.country == country)
        node = session.exec(q).first()
        if not node:
            raise HTTPException(status_code=404, detail="no node available")
        return {"node_id": node.node_id, "ip": node.ip, "country": node.country}

# WebSocket for agent management
@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONNECTIONS[node_id] = websocket
    try:
        while True:
            data = await websocket.receive_text()
            print(f"WS from {node_id}: {data}")
            await websocket.send_text(f"ack: {data}")
    except WebSocketDisconnect:
        WS_CONNECTIONS.pop(node_id, None)
        print(f"WS disconnected: {node_id}")

# Send command to a node
@app.post("/send-command/{node_id}")
async def send_command(node_id: str, cmd: str):
    ws = WS_CONNECTIONS.get(node_id)
    if not ws:
        raise HTTPException(status_code=404, detail="node not connected")
    await ws.send_text(cmd)
    return {"status": "sent"}

# ----------------------
# Proxy routing logic
# ----------------------
@app.websocket("/ws/proxy/{node_id}")
async def ws_proxy(websocket: WebSocket, node_id: str):
    """
    This endpoint accepts WebSocket connections from agents for SOCKS5 traffic.
    It supports multi-node routing: the control plane can forward requests
    to any connected node instead of the originating agent.
    """
    await websocket.accept()
    print(f"Proxy WS connected: {node_id}")
    try:
        # First message must be {"cmd":"connect","host":"example.com","port":80}
        msg = await websocket.receive_text()
        cmd = json.loads(msg)
        if cmd.get("cmd") != "connect":
            await websocket.send_text(json.dumps({"error":"first message must be connect"}))
            await websocket.close()
            return

        target_host = cmd["host"]
        target_port = int(cmd.get("port", 80))

        # Select a node for multi-node routing (random pick)
        available_nodes = [n for n in WS_CONNECTIONS.keys() if n != node_id]
        if available_nodes:
            chosen_node_id = random.choice(available_nodes)
            node_ws = WS_CONNECTIONS[chosen_node_id]
        else:
            # fallback: use the originating node
            chosen_node_id = node_id
            node_ws = websocket

        # Send connect command to chosen node if it's not the current websocket
        if chosen_node_id != node_id:
            await node_ws.send_text(json.dumps({"cmd":"connect","host":target_host,"port":target_port}))
            # Wait for confirmation
            resp_msg = await node_ws.receive_text()
            resp = json.loads(resp_msg)
            if resp.get("status") != "connected":
                await websocket.send_text(json.dumps({"error":"node failed to connect"}))
                await websocket.close()
                return

        # TCP connection from the chosen node
        reader, writer = await asyncio.open_connection(target_host, target_port)

        # Inform original agent we're connected
        await websocket.send_text(json.dumps({"status":"connected"}))

        # ws -> TCP
        async def ws_to_sock(ws_conn, tcp_writer):
            try:
                while True:
                    data = await ws_conn.receive_bytes()
                    if data is None:
                        break
                    tcp_writer.write(data)
                    await tcp_writer.drain()
            except Exception:
                pass
            finally:
                tcp_writer.close()

        # TCP -> ws
        async def sock_to_ws(ws_conn, tcp_reader):
            try:
                while True:
                    data = await tcp_reader.read(4096)
                    if not data:
                        break
                    await ws_conn.send_bytes(data)
            except Exception:
                pass
            finally:
                try:
                    await ws_conn.close()
                except:
                    pass

        await asyncio.gather(
            ws_to_sock(websocket, writer),
            sock_to_ws(websocket, reader)
        )

    except Exception as e:
        try:
            await websocket.send_text(json.dumps({"error": str(e)}))
        except:
            pass
        try:
            await websocket.close()
        except:
            pass
        print(f"Proxy WS error: {e}")
