# server.py
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlmodel import SQLModel, Session, create_engine, select
from typing import Dict, Deque, Optional
from collections import deque

import asyncio
import contextlib
import json
import os
import uuid

from models import Node  # keep using your existing model

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./nodes.db")
engine = create_engine(DATABASE_URL, echo=False)

app = FastAPI(title="Community Proxy Control Plane")

# ----------------------
# Connection registries
# ----------------------
# Control channels per node (for commands from server -> agent)
WS_CONTROL: Dict[str, WebSocket] = {}

# Pending tunnel futures: exit agent will attach the data WS here
PENDING_TUNNELS: Dict[str, asyncio.Future] = {}

# Round-robin pool of exit candidates (node_ids with active control WS)
EXIT_POOL: Deque[str] = deque()


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)


# ----------------------
# Node registration & heartbeat
# ----------------------
@app.post("/register")
async def register(node_id: str, public_key: str | None = None, ip: str | None = None, country: str | None = None):
    with Session(engine) as session:
        existing = session.exec(select(Node).where(Node.node_id == node_id)).first()
        now = datetime.now(timezone.utc)
        if existing:
            existing.public_key = public_key or existing.public_key
            existing.ip = ip or existing.ip
            existing.country = country or existing.country
            existing.last_seen = now
            session.add(existing)
            session.commit()
            return {"status": "ok", "node_id": node_id}

        node = Node(node_id=node_id, public_key=public_key, ip=ip, country=country, last_seen=now)
        session.add(node)
        session.commit()
        return {"status": "registered", "node_id": node_id}


@app.post("/heartbeat")
async def heartbeat(node_id: str):
    with Session(engine) as session:
        node = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if not node:
            raise HTTPException(status_code=404, detail="node not found")
        node.last_seen = datetime.now(timezone.utc)
        session.add(node)
        session.commit()
        return {"status": "ok"}


# ----------------------
# Simple matcher (unchanged semantics)
# ----------------------
@app.get("/match")
async def match(credits_required: int = 0, country: str | None = None):
    with Session(engine) as session:
        q = select(Node).where(Node.banned == False)
        if country:
            q = q.where(Node.country == country)
        node = session.exec(q).first()
        if not node:
            raise HTTPException(status_code=404, detail="no node available")
        return {"node_id": node.node_id, "ip": node.ip, "country": node.country}


# ----------------------
# WebSocket: control channel per node
# ----------------------
@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONTROL[node_id] = websocket
    if node_id not in EXIT_POOL:
        EXIT_POOL.append(node_id)

    # Mark node online in DB
    with Session(engine) as session:
        node = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if node:
            node.last_seen = datetime.now(timezone.utc)
            session.add(node)
            session.commit()

    try:
        while True:
            # Optional: handle keepalives or small control msgs from agent
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        WS_CONTROL.pop(node_id, None)
        with contextlib.suppress(ValueError):
            EXIT_POOL.remove(node_id)


# ----------------------
# WebSocket: exit agent attaches the data tunnel here
# ----------------------
@app.websocket("/ws/tunnel/{node_id}/{tunnel_id}")
async def ws_tunnel(websocket: WebSocket, node_id: str, tunnel_id: str):
    await websocket.accept()
    fut = PENDING_TUNNELS.get(tunnel_id)
    if fut and not fut.done():
        fut.set_result(websocket)
    else:
        # No pending waiter; close
        await websocket.close()


# ----------------------
# WebSocket: origin agent (SOCKS client side) connects here per target
# Server selects an exit node (round-robin) and bridges origin_ws <-> exit_ws
# ----------------------
@app.websocket("/ws/proxy/{node_id}")
async def ws_proxy(websocket: WebSocket, node_id: str):
    await websocket.accept()
    try:
        # First message must be a JSON connect command
        first_msg = await websocket.receive_text()
        try:
            first = json.loads(first_msg)
        except Exception:
            await websocket.send_text(json.dumps({"error": "invalid json"}))
            return await websocket.close()
        if first.get("cmd") != "connect":
            await websocket.send_text(json.dumps({"error": "first message must be connect"}))
            return await websocket.close()

        target_host = first["host"]
        target_port = int(first.get("port", 80))

        # Round-robin exit node selection
        if not EXIT_POOL:
            await websocket.send_text(json.dumps({"error": "no exit nodes available"}))
            return await websocket.close()

        chosen_id: Optional[str] = None
        for _ in range(len(EXIT_POOL)):
            candidate = EXIT_POOL[0]
            EXIT_POOL.rotate(-1)
            if candidate in WS_CONTROL:
                # Avoid self-egress if you want strict multi-node; allow if only node
                if candidate != node_id or len(EXIT_POOL) == 1:
                    chosen_id = candidate
                    break

        if not chosen_id:
            await websocket.send_text(json.dumps({"error": "no valid exit node found"}))
            return await websocket.close()

        control_ws = WS_CONTROL.get(chosen_id)
        if not control_ws:
            await websocket.send_text(json.dumps({"error": "exit node not connected"}))
            return await websocket.close()

        # Create pending tunnel & ask exit to connect
        tunnel_id = uuid.uuid4().hex
        fut = asyncio.get_event_loop().create_future()
        PENDING_TUNNELS[tunnel_id] = fut

        cmd = {"cmd": "connect", "host": target_host, "port": target_port, "tunnel": tunnel_id}
        await control_ws.send_text(json.dumps(cmd))

        # Wait for exit agent to attach the tunnel
        try:
            exit_ws: WebSocket = await asyncio.wait_for(fut, timeout=10)
        except asyncio.TimeoutError:
            PENDING_TUNNELS.pop(tunnel_id, None)
            await websocket.send_text(json.dumps({"error": "exit node did not open tunnel"}))
            return await websocket.close()
        finally:
            PENDING_TUNNELS.pop(tunnel_id, None)

        # Exit node sends a small status JSON first
        status_msg = await exit_ws.receive_text()
        try:
            status = json.loads(status_msg)
        except Exception:
            status = {"status": "connected"}
        if status.get("status") != "connected":
            await websocket.send_text(json.dumps({"error": "exit connection failed"}))
            with contextlib.suppress(Exception):
                await exit_ws.close()
            return await websocket.close()

        # Inform origin
        await websocket.send_text(json.dumps({"status": "connected", "exit": chosen_id}))

        async def pump(src: WebSocket, dst: WebSocket):
            try:
                while True:
                    data = await src.receive_bytes()
                    await dst.send_bytes(data)
            except Exception:
                pass
            finally:
                with contextlib.suppress(Exception):
                    await dst.close()

        async def pump_rev(src: WebSocket, dst: WebSocket):
            try:
                while True:
                    data = await src.receive_bytes()
                    await dst.send_bytes(data)
            except Exception:
                pass
            finally:
                with contextlib.suppress(Exception):
                    await dst.close()

        await asyncio.gather(
            pump(websocket, exit_ws),
            pump_rev(exit_ws, websocket),
        )

    except Exception as e:
        with contextlib.suppress(Exception):
            await websocket.send_text(json.dumps({"error": str(e)}))
        with contextlib.suppress(Exception):
            await websocket.close()
