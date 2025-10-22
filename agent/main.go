package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/armon/go-socks5"
	"github.com/gorilla/websocket"
)

var (
	serverURL string
	nodeID    string
)

func init() {
	flag.StringVar(&serverURL, "server", "http://localhost:8000", "control plane URL")
	flag.StringVar(&nodeID, "id", "", "node id (auto if empty)")
	flag.Parse()
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", time.Now().Unix())
	}
}

// Register node with control plane
func register() error {
	url := fmt.Sprintf("%s/register?node_id=%s&ip=127.0.0.1", serverURL, nodeID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Printf("✅ Registered node %s with control plane", nodeID)
	return nil
}

// Heartbeat loop
func heartbeatLoop() {
	for {
		url := fmt.Sprintf("%s/heartbeat?node_id=%s", serverURL, nodeID)
		_, err := http.Post(url, "application/json", nil)
		if err != nil {
			log.Printf("💔 Heartbeat error: %v", err)
		}
		time.Sleep(10 * time.Second)
	}
}

// --- WebSocket connection wrapper implementing net.Conn ---
type websocketConn struct {
	conn *websocket.Conn
}

func (w *websocketConn) Read(p []byte) (int, error) {
	_, data, err := w.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	return n, nil
}

func (w *websocketConn) Write(p []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *websocketConn) Close() error { return w.conn.Close() }

func (w *websocketConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
}

func (w *websocketConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
}

func (w *websocketConn) SetDeadline(t time.Time) error      { return nil }
func (w *websocketConn) SetReadDeadline(t time.Time) error  { return nil }
func (w *websocketConn) SetWriteDeadline(t time.Time) error { return nil }

// --- SOCKS5 proxy logic ---
func startSocks(local string) error {
	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Ask control plane which node to use
			matchURL := fmt.Sprintf("%s/match", serverURL)
			resp, err := http.Get(matchURL)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			var node struct {
				NodeID  string `json:"node_id"`
				IP      string `json:"ip"`
				Country string `json:"country"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
				return nil, err
			}
			log.Printf("🌐 Control plane selected node: %s (%s)", node.NodeID, node.IP)

			// Local node → direct connect
			if node.NodeID == nodeID {
				log.Printf("➡️ Using local node for %s", addr)
				return net.Dial(network, addr)
			}

			// Remote node → connect via WebSocket
			wsurl := serverURL
			if wsurl[:5] == "http:" {
				wsurl = "ws:" + wsurl[5:]
			} else if wsurl[:6] == "https:" {
				wsurl = "wss:" + wsurl[6:]
			}
			wsurl = fmt.Sprintf("%s/ws/proxy/%s", wsurl, node.NodeID)

			dialer := websocket.DefaultDialer
			c, _, err := dialer.Dial(wsurl, nil)
			if err != nil {
				return nil, err
			}

			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
				port = "80"
			}

			cmd := map[string]interface{}{"cmd": "connect", "host": host, "port": port}
			cmdj, _ := json.Marshal(cmd)
			if err := c.WriteMessage(websocket.TextMessage, cmdj); err != nil {
				c.Close()
				return nil, err
			}

			_, msg, err := c.ReadMessage()
			if err != nil {
				c.Close()
				return nil, err
			}
			var respj map[string]interface{}
			_ = json.Unmarshal(msg, &respj)
			if respj["status"] != "connected" {
				c.Close()
				return nil, fmt.Errorf("remote proxy failed: %v", respj)
			}

			return &websocketConn{conn: c}, nil
		},
	}

	server, err := socks5.New(conf)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", local)
	if err != nil {
		return err
	}

	log.Printf("🧦 SOCKS5 proxy listening on %s", local)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept err: %v", err)
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			server.ServeConn(c)
		}(conn)
	}
}

func main() {
	log.Printf("🚀 Starting agent %s", nodeID)
	if err := register(); err != nil {
		log.Fatalf("Register failed: %v", err)
	}
	go heartbeatLoop()

	if err := startSocks("127.0.0.1:1080"); err != nil {
		log.Fatalf("SOCKS5 failed: %v", err)
	}
}
