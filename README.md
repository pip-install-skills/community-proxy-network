# Community Proxy Network — PoC Skeleton

This repo is a minimal, **safe prototype** showing the control-plane + edge-agent skeleton for an opt-in community proxy network. It is intentionally small: it implements node registration, heartbeat, matchmaking, and a WebSocket control channel for future commands. Proxying/data-plane is left as a guided TODO with pointers.

> **Goal:** give you working code you can run locally (VMs or containers) to register agents and see them listed by the control plane. Use this as the base to implement secure tunnels (WireGuard, WebRTC) and the actual SOCKS5/HTTP proxying later.

---

## File tree

```
community-proxy-poc/
├── README.md                       # this file
├── docker-compose.yml              # optional: run control plane + sqlite in container
├── control_plane/
│   ├── main.py                     # FastAPI control plane
│   ├── models.py                   # SQLModel data models
│   ├── requirements.txt
│   └── README.md
└── agent/
    ├── main.go                     # Go agent skeleton: register + websocket control channel
    ├── go.mod
    └── README.md
```

---

## Quick summary
- **Control plane**: FastAPI (Python) + SQLModel (SQLite) — endpoints: `/register`, `/heartbeat`, `/match`, and `/ws/node/{node_id}` (WebSocket for control messages).
- **Agent**: Go program — registers to control plane, sends heartbeat, maintains a WebSocket to receive control commands (future: accept proxy tasks). Uses environment config for server URL and node metadata.

This is intentionally a prototype (safe): it does **not** forward arbitrary third-party traffic yet. Use this to iterate on NAT traversal, auth, and policy before implementing exit forwarding.

---

# Control Plane

## `control_plane/requirements.txt`
```
fastapi
uvicorn[standard]
sqlmodel
httpx
python-dotenv
```

## `control_plane/models.py`
```python
from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

class Node(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    node_id: str
    public_key: Optional[str] = None
    ip: Optional[str] = None
    nat_type: Optional[str] = None
    country: Optional[str] = None
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    credits: int = 0
    banned: bool = False

class MatchRequest(SQLModel):
    credits_required: int = 0
    country: Optional[str] = None

```

## `control_plane/main.py`
```python
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
```

## `control_plane/README.md`
```
# Control Plane

Run locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Endpoints:
- POST /register?node_id=<id>&public_key=<pk>&ip=<ip>&country=<cc>
- POST /heartbeat?node_id=<id>
- GET /match?credits_required=0
- WebSocket: /ws/node/{node_id}
- POST /send-command/{node_id}?cmd=...
```

---

# Agent (Go)

The agent registers to the control plane and keeps a WebSocket alive to receive control messages. It also exposes a local SOCKS5 endpoint TODO (sketched below) — see `agent/README.md` for next steps.

## `agent/go.mod`
```
module community-proxy-agent

go 1.21

require (
    github.com/gorilla/websocket v1.5.0
    github.com/armon/go-socks5 v0.0.0-20190316174220-6d5f6b7b5d1c
)
```

## `agent/main.go`
```go
package main

import (
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/gorilla/websocket"
)

var serverURL string
var nodeID string

func init() {
    flag.StringVar(&serverURL, "server", "http://localhost:8000", "control plane URL")
    flag.StringVar(&nodeID, "id", "", "node id (auto if empty)")
    flag.Parse()
    if nodeID == "" {
        nodeID = fmt.Sprintf("node-%d", time.Now().Unix())
    }
}

func register() error {
    url := fmt.Sprintf("%s/register?node_id=%s&ip=127.0.0.1", serverURL, nodeID)
    resp, err := http.Post(url, "application/json", nil)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    log.Printf("registered: %s -> %s", nodeID, serverURL)
    return nil
}

func heartbeatLoop() {
    for {
        url := fmt.Sprintf("%s/heartbeat?node_id=%s", serverURL, nodeID)
        _, err := http.Post(url, "application/json", nil)
        if err != nil {
            log.Printf("heartbeat err: %v", err)
        }
        time.Sleep(10 * time.Second)
    }
}

func wsLoop() {
    wsurl := serverURL
    // switch http -> ws
    if wsurl[:5] == "http:" {
        wsurl = "ws:" + wsurl[5:]
    } else if wsurl[:6] == "https:" {
        wsurl = "wss:" + wsurl[6:]
    }
    wsurl = fmt.Sprintf("%s/ws/node/%s", wsurl, nodeID)
    for {
        c, _, err := websocket.DefaultDialer.Dial(wsurl, nil)
        if err != nil {
            log.Printf("ws dial error: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        log.Printf("ws connected %s", wsurl)
        for {
            _, message, err := c.ReadMessage()
            if err != nil {
                log.Printf("ws read error: %v", err)
                break
            }
            log.Printf("ws msg: %s", string(message))
            // TODO: handle control commands like "open-tunnel", "shutdown", etc.
        }
        c.Close()
        time.Sleep(2 * time.Second)
    }
}

func main() {
    log.Printf("starting agent %s", nodeID)
    if err := register(); err != nil {
        log.Printf("register failed: %v", err)
        os.Exit(1)
    }

    go heartbeatLoop()
    go wsLoop()

    // placeholder: keep running
    select {}
}
```

## `agent/README.md`
```
# Agent

Build & run:

```bash
cd agent
go mod download
go build -o agent
./agent -server http://localhost:8000 -id my-node-1
```

The agent will:
- POST /register
- POST /heartbeat every 10s
- Connect to WebSocket /ws/node/{node_id} and print received messages

Next steps (TODO):
- Add local SOCKS5 listener and forward connections over an encrypted tunnel to a matched exit node.
- Implement mutual TLS or WireGuard key exchange for secure tunnels.
- Implement resource caps, per-node exit policy, and automated quarantine logic.
```

---

# docker-compose.yml (optional)

```yaml
version: "3.8"
services:
  control:
    build: ./control_plane
    command: uvicorn main:app --host 0.0.0.0 --port 8000
    ports:
      - "8000:8000"
    volumes:
      - ./control_plane:/app
```

---

# Next steps I recommend you do immediately
1. Run this PoC locally and confirm agents register and WebSocket messages are delivered.  
2. Replace the WebSocket command channel with a mutual‑TLS or WireGuard control channel for authenticated tunnels.  
3. Add SOCKS5 local listener in the agent and implement an authenticated, encrypted tunnel between a client and exit node (WireGuard, or multi-hop TLS over WebRTC).  
4. Implement `match` logic that enforces credits and exit policy before returning node candidates.  

---

If you'd like, I can now:
- generate a runnable Dockerfile and a docker-compose that actually starts FastAPI + agent containers and demonstrates registration, **or**
- convert the PoC's proxy TODO into a working local SOCKS5-over-WebSocket relay (so the agent actually proxies traffic through another agent via the control plane relay) — note: this will be purely PoC and not production secure.

Tell me which of those two you'd like next and I will create it here.

