from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlmodel import SQLModel, Session, create_engine, select
from typing import Dict, Deque, Optional
from collections import deque
import asyncio
import contextlib
import json
import logging
import os
import uuid

from models import Node

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./nodes.db")
engine = create_engine(DATABASE_URL, echo=False)

app = FastAPI(title="Community Proxy Control Plane")
log = logging.getLogger("uvicorn.error")
log.setLevel(logging.DEBUG)

WS_CONTROL: Dict[str, WebSocket] = {}
PENDING_TUNNELS: Dict[str, asyncio.Future] = {}
EXIT_POOL: Deque[str] = deque()


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)


@app.post("/register")
async def register(node_id: str, public_key: str | None = None, ip: str | None = None, country: str | None = None):
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        node = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if node:
            node.public_key = public_key or node.public_key
            node.ip = ip or node.ip
            node.country = country or node.country
            node.last_seen = now
            session.add(node)
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


@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONTROL[node_id] = websocket
    if node_id not in EXIT_POOL:
        EXIT_POOL.append(node_id)

    with Session(engine) as session:
        node = session.exec(select(Node).where(Node.node_id == node_id)).first()
        if node:
            node.last_seen = datetime.now(timezone.utc)
            session.add(node)
            session.commit()

    log.info("control connected: %s", node_id)
    try:
        while True:
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        WS_CONTROL.pop(node_id, None)
        with contextlib.suppress(ValueError):
            EXIT_POOL.remove(node_id)
        log.info("control disconnected: %s", node_id)


@app.websocket("/ws/tunnel/{node_id}/{tunnel_id}")
async def ws_tunnel(websocket: WebSocket, node_id: str, tunnel_id: str):
    await websocket.accept()
    fut = PENDING_TUNNELS.get(tunnel_id)
    if fut and not fut.done():
        log.debug("Attaching tunnel websocket: exit=%s tunnel_id=%s fut=%s", node_id, tunnel_id, repr(fut))
        try:
            fut.set_result(websocket)
            log.info("tunnel attached: exit=%s tunnel_id=%s", node_id, tunnel_id)
        except Exception as e:
            log.exception("error setting tunnel future result: %s", e)
    else:
        await websocket.close()
        log.warning("tunnel with no waiter: exit=%s tunnel_id=%s (closed)", node_id, tunnel_id)


@app.websocket("/ws/proxy/{node_id}")
async def ws_proxy(websocket: WebSocket, node_id: str):
    await websocket.accept()
    exit_ws: Optional[WebSocket] = None
    tunnel_id: Optional[str] = None
    
    try:
        first_msg = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        try:
            first = json.loads(first_msg)
        except Exception as e:
            log.error(f"JSON parse error: {e}")
            await websocket.send_text(json.dumps({"error": "invalid json"}))
            return await websocket.close()
        
        if first.get("cmd") != "connect":
            await websocket.send_text(json.dumps({"error": "first message must be connect"}))
            return await websocket.close()

        target_host = first["host"]
        target_port = int(first.get("port", 80))

        if not EXIT_POOL:
            await websocket.send_text(json.dumps({"error": "no exit nodes available"}))
            return await websocket.close()

        chosen_id: Optional[str] = None
        for _ in range(len(EXIT_POOL)):
            cand = EXIT_POOL[0]
            EXIT_POOL.rotate(-1)
            if cand in WS_CONTROL and (cand != node_id or len(EXIT_POOL) == 1):
                chosen_id = cand
                break
        
        if not chosen_id:
            await websocket.send_text(json.dumps({"error": "no valid exit node found"}))
            return await websocket.close()

        control_ws = WS_CONTROL.get(chosen_id)
        if not control_ws:
            await websocket.send_text(json.dumps({"error": "exit node not connected"}))
            return await websocket.close()

        log.info("proxy: origin=%s -> exit=%s target=%s:%s", node_id, chosen_id, target_host, target_port)

        tunnel_id = uuid.uuid4().hex
        fut = asyncio.get_event_loop().create_future()
        PENDING_TUNNELS[tunnel_id] = fut

        cmd = {"cmd": "connect", "host": target_host, "port": target_port, "tunnel": tunnel_id}
        await control_ws.send_text(json.dumps(cmd))

        try:
            exit_ws = await asyncio.wait_for(fut, timeout=15)
        except asyncio.TimeoutError:
            PENDING_TUNNELS.pop(tunnel_id, None)
            log.error(f"Timeout waiting for tunnel {tunnel_id}")
            await websocket.send_text(json.dumps({"error": "exit node did not open tunnel"}))
            return await websocket.close()
        finally:
            PENDING_TUNNELS.pop(tunnel_id, None)

        await websocket.send_text(json.dumps({"status": "connected", "exit": chosen_id}))
        log.debug(f"Tunnel {tunnel_id} ready, starting relay")

        # Improved relay with better error handling
        done = asyncio.Event()
        bytes_origin = 0
        bytes_exit = 0

        async def relay_origin_to_exit():
            nonlocal bytes_origin
            try:
                while not done.is_set():
                    data = await websocket.receive_bytes()
                    await exit_ws.send_bytes(data)
                    bytes_origin += len(data)
            except WebSocketDisconnect:
                log.debug(f"Origin disconnected (sent {bytes_origin} bytes)")
            except Exception as e:
                log.debug(f"Origin relay error: {type(e).__name__}: {e}")
            finally:
                done.set()

        async def relay_exit_to_origin():
            nonlocal bytes_exit
            try:
                while not done.is_set():
                    data = await exit_ws.receive_bytes()
                    await websocket.send_bytes(data)
                    bytes_exit += len(data)
            except WebSocketDisconnect:
                log.debug(f"Exit disconnected (sent {bytes_exit} bytes)")
            except Exception as e:
                log.debug(f"Exit relay error: {type(e).__name__}: {e}")
            finally:
                done.set()

        tasks = [
            asyncio.create_task(relay_origin_to_exit()),
            asyncio.create_task(relay_exit_to_origin())
        ]

        await done.wait()
        
        # Cancel remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        log.info(f"Tunnel {tunnel_id} closed. Origin→Exit: {bytes_origin} bytes, Exit→Origin: {bytes_exit} bytes")

    except asyncio.TimeoutError:
        log.error("Timeout waiting for initial connect message")
        with contextlib.suppress(Exception):
            await websocket.send_text(json.dumps({"error": "timeout"}))
    except Exception as e:
        log.error(f"ws_proxy error: {type(e).__name__}: {e}", exc_info=True)
        with contextlib.suppress(Exception):
            await websocket.send_text(json.dumps({"error": str(e)}))
    finally:
        with contextlib.suppress(Exception):
            await websocket.close()
        if exit_ws:
            with contextlib.suppress(Exception):
                await exit_ws.close()