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

func register() error {
	url := fmt.Sprintf("%s/register?node_id=%s&ip=127.0.0.1", serverURL, nodeID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
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

// --- WebSocket read/write adapter ---
type websocketReadWriteCloser struct {
	conn *websocket.Conn
}

func (w *websocketReadWriteCloser) Read(p []byte) (int, error) {
	_, data, err := w.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	return n, nil
}

func (w *websocketReadWriteCloser) Write(p []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *websocketReadWriteCloser) Close() error { return w.conn.Close() }
func (w *websocketReadWriteCloser) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1080}
}
func (w *websocketReadWriteCloser) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1081}
}
func (w *websocketReadWriteCloser) SetDeadline(t time.Time) error      { return nil }
func (w *websocketReadWriteCloser) SetReadDeadline(t time.Time) error  { return nil }
func (w *websocketReadWriteCloser) SetWriteDeadline(t time.Time) error { return nil }

// --- Actual distributed proxy logic ---
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
			log.Printf("Control plane selected node: %s (%s)", node.NodeID, node.IP)

			// If the control plane chose *this* node, connect directly
			if node.NodeID == nodeID {
				log.Printf("Using local node for %s", addr)
				return net.Dial(network, addr)
			}

			// Otherwise, connect through selected remote node via control plane WebSocket
			wsurl := serverURL
			if wsurl[:5] == "http:" {
				wsurl = "ws:" + wsurl[5:]
			} else if wsurl[:6] == "https:" {
				wsurl = "wss:" + wsurl[6:]
			}
			wsurl = fmt.Sprintf("%s/ws/proxy/%s", wsurl, node.NodeID)

			header := http.Header{}
			dialer := websocket.DefaultDialer
			c, _, err := dialer.Dial(wsurl, header)
			if err != nil {
				return nil, err
			}

			// Split host/port
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

			// Wait for "connected"
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

			return &websocketReadWriteCloser{conn: c}, nil
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

	log.Printf("SOCKS5 listening on %s", local)
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
	log.Printf("starting agent %s", nodeID)
	if err := register(); err != nil {
		log.Fatalf("register failed: %v", err)
	}
	go heartbeatLoop()

	if err := startSocks("127.0.0.1:1080"); err != nil {
		log.Fatalf("socks failed: %v", err)
	}
}
