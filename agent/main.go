// main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

func httpToWS(url string) string {
	if len(url) >= 6 && url[:6] == "https:" {
		return "wss:" + url[6:]
	}
	return "ws:" + url[5:] // http: -> ws:
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

func controlLoop() {
	wsURL := fmt.Sprintf("%s/ws/node/%s", httpToWS(serverURL), nodeID)
	for {
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("control dial err: %v, retrying...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("control connected")

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				log.Printf("control read err: %v", err)
				_ = c.Close()
				break
			}

			// Be tolerant of port type (string/number)
			var m map[string]any
			if err := json.Unmarshal(msg, &m); err != nil {
				continue
			}
			if cmd, _ := m["cmd"].(string); cmd != "connect" {
				continue
			}
			host, _ := m["host"].(string)
			tunnel, _ := m["tunnel"].(string)
			port := readPortString(m["port"])
			if host == "" || port == "" || tunnel == "" {
				continue
			}
			go handleConnect(host, port, tunnel)
		}
	}
}

func readPortString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return fmt.Sprintf("%.0f", t)
	case json.Number:
		return string(t)
	default:
		return ""
	}
}

func handleConnect(host, port, tunnel string) {
	// 1) TCP from this agent (egress = agent host)
	addr := net.JoinHostPort(host, port)
	tcp, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		log.Printf("tcp dial err to %s: %v", addr, err)
		return
	}

	// 2) Open tunnel WS back to control plane
	wsURL := fmt.Sprintf("%s/ws/tunnel/%s/%s", httpToWS(serverURL), nodeID, tunnel)
	tun, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("tunnel dial err: %v", err)
		_ = tcp.Close()
		return
	}

	// 3) Signal ready
	ready, _ := json.Marshal(map[string]string{"status": "connected"})
	if err := tun.WriteMessage(websocket.TextMessage, ready); err != nil {
		log.Printf("tunnel status write err: %v", err)
		_ = tun.Close()
		_ = tcp.Close()
		return
	}

	// 4) Pump bytes both ways
	errCh := make(chan error, 2)

	// WS -> TCP
	go func() {
		for {
			mt, data, err := tun.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			if _, err := tcp.Write(data); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// TCP -> WS
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := tcp.Read(buf)
			if n > 0 {
				if err := tun.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
					errCh <- err
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				} else {
					errCh <- nil
				}
				return
			}
		}
	}()

	<-errCh
	_ = tun.Close()
	_ = tcp.Close()
}

// ---- websocketReadWriteCloser implements net.Conn ----
type websocketReadWriteCloser struct {
	conn *websocket.Conn
}

func (w *websocketReadWriteCloser) Read(p []byte) (int, error) {
	_, data, err := w.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	return copy(p, data), nil
}

func (w *websocketReadWriteCloser) Write(p []byte) (int, error) {
	if err := w.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *websocketReadWriteCloser) Close() error { return w.conn.Close() }

// Satisfy net.Conn:
func (w *websocketReadWriteCloser) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1080}
}
func (w *websocketReadWriteCloser) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1081}
}
func (w *websocketReadWriteCloser) SetDeadline(t time.Time) error      { return nil }
func (w *websocketReadWriteCloser) SetReadDeadline(t time.Time) error  { return nil }
func (w *websocketReadWriteCloser) SetWriteDeadline(t time.Time) error { return nil }

func startSocks(local string) error {
	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Per-connection proxy WS to control plane; server picks exit node
			wsurl := fmt.Sprintf("%s/ws/proxy/%s", httpToWS(serverURL), nodeID)
			c, _, err := websocket.DefaultDialer.Dial(wsurl, nil)
			if err != nil {
				return nil, err
			}

			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
				port = "80"
			}
			cmd := map[string]any{"cmd": "connect", "host": host, "port": port}
			cmdj, _ := json.Marshal(cmd)
			if err := c.WriteMessage(websocket.TextMessage, cmdj); err != nil {
				_ = c.Close()
				return nil, err
			}

			// Wait for connected ACK
			_, msg, err := c.ReadMessage()
			if err != nil {
				_ = c.Close()
				return nil, err
			}
			var resp map[string]any
			_ = json.Unmarshal(msg, &resp)
			if s, _ := resp["status"].(string); s != "connected" {
				_ = c.Close()
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
	go controlLoop()

	// Local SOCKS5 for this host's apps
	go func() {
		if err := startSocks("127.0.0.1:1080"); err != nil {
			log.Fatalf("socks failed: %v", err)
		}
	}()

	select {}
}
