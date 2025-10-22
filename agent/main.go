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

// register node with control plane
func register() error {
	url := fmt.Sprintf("%s/register?node_id=%s&ip=127.0.0.1", serverURL, nodeID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// websocketReadWriteCloser adapts gorilla websocket to a net.Conn
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

func (w *websocketReadWriteCloser) Close() error {
	return w.conn.Close()
}

// Important: return dummy but non-nil addresses for SOCKS5
func (w *websocketReadWriteCloser) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1080}
}
func (w *websocketReadWriteCloser) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1081}
}
func (w *websocketReadWriteCloser) SetDeadline(t time.Time) error      { return nil }
func (w *websocketReadWriteCloser) SetReadDeadline(t time.Time) error  { return nil }
func (w *websocketReadWriteCloser) SetWriteDeadline(t time.Time) error { return nil }

// wrappedConn preserves actual client addresses for SOCKS5
type wrappedConn struct {
	net.Conn
	localAddr, remoteAddr net.Addr
}

func (w *wrappedConn) LocalAddr() net.Addr                { return w.localAddr }
func (w *wrappedConn) RemoteAddr() net.Addr               { return w.remoteAddr }
func (w *wrappedConn) SetDeadline(t time.Time) error      { return w.Conn.SetDeadline(t) }
func (w *wrappedConn) SetReadDeadline(t time.Time) error  { return w.Conn.SetReadDeadline(t) }
func (w *wrappedConn) SetWriteDeadline(t time.Time) error { return w.Conn.SetWriteDeadline(t) }

func startSocks(local string) error {
	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Convert HTTP(S) URL to WS(WSS)
			wsurl := serverURL
			if wsurl[:5] == "http:" {
				wsurl = "ws:" + wsurl[5:]
			} else if wsurl[:6] == "https:" {
				wsurl = "wss:" + wsurl[6:]
			}
			wsurl = fmt.Sprintf("%s/ws/proxy/%s", wsurl, nodeID)

			header := http.Header{}
			dialer := websocket.DefaultDialer
			c, _, err := dialer.Dial(wsurl, header)
			if err != nil {
				return nil, err
			}

			// Split addr into host and port
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
				port = "80"
			}

			// Send connect request over WebSocket
			cmd := map[string]interface{}{"cmd": "connect", "host": host, "port": port}
			cmdj, _ := json.Marshal(cmd)
			if err := c.WriteMessage(websocket.TextMessage, cmdj); err != nil {
				c.Close()
				return nil, err
			}

			// Wait for confirmation
			_, msg, err := c.ReadMessage()
			if err != nil {
				c.Close()
				return nil, err
			}
			var resp map[string]interface{}
			_ = json.Unmarshal(msg, &resp)
			if resp["status"] != "connected" {
				c.Close()
				return nil, fmt.Errorf("proxy not connected: %v", resp)
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

		wrapped := &wrappedConn{
			Conn:       conn,
			localAddr:  conn.LocalAddr(),
			remoteAddr: conn.RemoteAddr(),
		}

		go func(c net.Conn) {
			defer c.Close()
			server.ServeConn(c)
		}(wrapped)
	}
}

// Heartbeat to keep node alive in control plane
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

func main() {
	log.Printf("starting agent %s", nodeID)
	if err := register(); err != nil {
		log.Fatalf("register failed: %v", err)
	}
	go heartbeatLoop()

	// Start SOCKS5 listener
	go func() {
		if err := startSocks("127.0.0.1:1080"); err != nil {
			log.Fatalf("socks failed: %v", err)
		}
	}()

	select {} // keep main alive
}
