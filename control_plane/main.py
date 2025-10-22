from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlmodel import SQLModel, Session, create_engine, select
from models import Node
import asyncio
import json
import os
import random
import socket

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./nodes.db")
engine = create_engine(DATABASE_URL, echo=False)
app = FastAPI(title="Community Proxy Control Plane")

WS_CONNECTIONS = {}      # management
RELAY_CONNECTIONS = {}   # data relay sockets
TCP_TARGETS = {}         # mapping for relay target sockets

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)

# --- Register and Heartbeat ---
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

# --- WebSocket Management Channels ---
@app.websocket("/ws/node/{node_id}")
async def ws_node(websocket: WebSocket, node_id: str):
    await websocket.accept()
    WS_CONNECTIONS[node_id] = websocket
    print(f"Mgmt node connected: {node_id}")
    try:
        while True:
            msg = await websocket.receive_text()
            print(f"[MGMT] {node_id}: {msg}")
    except WebSocketDisconnect:
        WS_CONNECTIONS.pop(node_id, None)
        print(f"Mgmt node disconnected: {node_id}")

# --- Relay Node (handles real outbound connections) ---
@app.websocket("/ws/relay/{node_id}")
async def ws_relay(websocket: WebSocket, node_id: str):
    """Relay node actually connects to target servers on the Internet"""
    await websocket.accept()
    RELAY_CONNECTIONS[node_id] = websocket
    print(f"Relay node connected: {node_id}")

    try:
        while True:
            msg = await websocket.receive_text()
            cmd = json.loads(msg)

            if cmd.get("cmd") == "connect":
                host = cmd["host"]
                port = int(cmd["port"])
                print(f"[{node_id}] connecting to {host}:{port}")

                try:
                    sock = socket.create_connection((host, port), timeout=5)
                    sock.setblocking(False)
                    TCP_TARGETS[node_id] = sock
                    await websocket.send_text(json.dumps({"status": "connected"}))
                except Exception as e:
                    await websocket.send_text(json.dumps({"error": str(e)}))
                    continue

                async def ws_to_tcp():
                    try:
                        while True:
                            data = await websocket.receive_bytes()
                            sock.sendall(data)
                    except Exception:
                        pass
                    try:
                        sock.close()
                    except:
                        pass

                async def tcp_to_ws():
                    loop = asyncio.get_event_loop()
                    try:
                        while True:
                            data = await loop.run_in_executor(None, sock.recv, 4096)
                            if not data:
                                break
                            await websocket.send_bytes(data)
                    except Exception:
                        pass
                    try:
                        await websocket.close()
                    except:
                        pass

                await asyncio.gather(ws_to_tcp(), tcp_to_ws())

    except WebSocketDisconnect:
        print(f"Relay node disconnected: {node_id}")
    finally:
        RELAY_CONNECTIONS.pop(node_id, None)
        if node_id in TCP_TARGETS:
            TCP_TARGETS[node_id].close()
            TCP_TARGETS.pop(node_id, None)

# --- Proxy Node (receives SOCKS requests) ---
@app.websocket("/ws/proxy/{node_id}")
async def ws_proxy(websocket: WebSocket, node_id: str):
    """Incoming SOCKS requests arrive here and are routed to relay nodes"""
    await websocket.accept()
    print(f"Proxy request from node {node_id}")

    try:
        msg = await websocket.receive_text()
        cmd = json.loads(msg)
        if cmd.get("cmd") != "connect":
            await websocket.send_text(json.dumps({"error": "first message must be connect"}))
            await websocket.close()
            return

        target_host = cmd["host"]
        target_port = int(cmd.get("port", 80))

        exit_nodes = [n for n in RELAY_CONNECTIONS.keys() if n != node_id]
        if not exit_nodes:
            await websocket.send_text(json.dumps({"error": "no exit nodes available"}))
            await websocket.close()
            return

        chosen_node = random.choice(exit_nodes)
        exit_ws = RELAY_CONNECTIONS[chosen_node]
        print(f"Routing traffic via exit node: {chosen_node}")

        # Ask exit node to connect
        await exit_ws.send_text(json.dumps({"cmd": "connect", "host": target_host, "port": target_port}))
        resp = await exit_ws.receive_text()
        respj = json.loads(resp)
        if "error" in respj:
            await websocket.send_text(json.dumps(respj))
            await websocket.close()
            return

        await websocket.send_text(json.dumps({"status": "connected", "exit": chosen_node}))

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
