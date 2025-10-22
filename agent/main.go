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
	"sync"
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
	return "ws:" + url[5:]
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
	dialer := websocket.Dialer{
		HandshakeTimeout:  30 * time.Second,
		EnableCompression: false,
	}

	for {
		c, _, err := dialer.Dial(wsURL, nil)
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
	addr := net.JoinHostPort(host, port)
	log.Printf("Connecting to %s for tunnel %s", addr, tunnel)

	tcp, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		log.Printf("tcp dial err to %s: %v", addr, err)
		return
	}
	defer tcp.Close()
	log.Printf("tcp connected to %s", addr)

	wsURL := fmt.Sprintf("%s/ws/tunnel/%s/%s", httpToWS(serverURL), nodeID, tunnel)
	dialer := websocket.Dialer{
		HandshakeTimeout:  30 * time.Second,
		EnableCompression: false,
	}
	tunWS, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("tunnel dial err: %v", err)
		return
	}
	defer tunWS.Close()
	log.Printf("tunnel websocket connected for %s", tunnel)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// TCP -> WebSocket
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 32768)
		totalBytes := 0
		for {
			select {
			case <-done:
				return
			default:
			}

			tcp.SetReadDeadline(time.Now().Add(2 * time.Minute))
			n, err := tcp.Read(buf)
			if n > 0 {
				totalBytes += n
				if err := tunWS.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
					log.Printf("ws write err (sent %d bytes): %v", totalBytes, err)
					close(done)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("tcp read err (sent %d bytes): %v", totalBytes, err)
				} else {
					log.Printf("tcp EOF (sent %d bytes)", totalBytes)
				}
				close(done)
				return
			}
		}
	}()

	// WebSocket -> TCP
	wg.Add(1)
	go func() {
		defer wg.Done()
		totalBytes := 0
		for {
			select {
			case <-done:
				return
			default:
			}

			tunWS.SetReadDeadline(time.Now().Add(2 * time.Minute))
			mt, data, err := tunWS.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Printf("tunWS normal close (received %d bytes)", totalBytes)
				} else {
					log.Printf("tunWS read err (received %d bytes): %v", totalBytes, err)
				}
				close(done)
				return
			}

			if mt != websocket.BinaryMessage {
				continue
			}

			totalBytes += len(data)
			tcp.SetWriteDeadline(time.Now().Add(2 * time.Minute))
			_, err = tcp.Write(data)
			if err != nil {
				log.Printf("tcp write err (received %d bytes): %v", totalBytes, err)
				close(done)
				return
			}
		}
	}()

	wg.Wait()
	log.Printf("tunnel %s closed", tunnel)
}

func startSocks(local string) error {
	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			wsurl := fmt.Sprintf("%s/ws/proxy/%s", httpToWS(serverURL), nodeID)

			dialer := websocket.Dialer{
				HandshakeTimeout:  15 * time.Second,
				EnableCompression: false,
			}
			c, _, err := dialer.Dial(wsurl, nil)
			if err != nil {
				return nil, fmt.Errorf("ws dial: %w", err)
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
				return nil, fmt.Errorf("ws write connect: %w", err)
			}

			// Wait for connected ACK
			c.SetReadDeadline(time.Now().Add(20 * time.Second))
			mt, msg, err := c.ReadMessage()
			c.SetReadDeadline(time.Time{})
			c.SetReadDeadline(time.Time{})
			if err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("ws read ack: %w", err)
			}

			if mt != websocket.TextMessage {
				_ = c.Close()
				return nil, fmt.Errorf("expected text ack, got type %d", mt)
			}

			var resp map[string]any
			if err := json.Unmarshal(msg, &resp); err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("ack json parse: %w", err)
			}

			if s, _ := resp["status"].(string); s != "connected" {
				_ = c.Close()
				return nil, fmt.Errorf("proxy not connected: %v", resp)
			}

			log.Printf("SOCKS connection established to %s via control plane", addr)
			return &wsConn{ws: c}, nil
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
			if err := server.ServeConn(c); err != nil {
				log.Printf("socks serve err: %v", err)
			}
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

	go func() {
		if err := startSocks("127.0.0.1:1080"); err != nil {
			log.Fatalf("socks failed: %v", err)
		}
	}()

	select {}
}

///////////////////////////////////////////////////////////////
// wsConn: WebSocket net.Conn wrapper - FIXED VERSION
///////////////////////////////////////////////////////////////

type wsConn struct {
	ws        *websocket.Conn
	readBuf   []byte
	readMu    sync.Mutex
	writeMu   sync.Mutex
	closed    bool
	closeMu   sync.Mutex
	closeOnce sync.Once
}

func (c *wsConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	// Return buffered data first
	if len(c.readBuf) > 0 {
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		return n, nil
	}

	// CRITICAL FIX: Keep reading until we get binary data or an error
	for {
		c.closeMu.Lock()
		if c.closed {
			c.closeMu.Unlock()
			return 0, io.EOF
		}
		c.closeMu.Unlock()

		mt, data, err := c.ws.ReadMessage()
		if err != nil {
			return 0, err
		}

		// Skip non-binary frames (pings, pongs, text) - DO NOT return 0!
		if mt != websocket.BinaryMessage {
			continue
		}

		// We got binary data - return it
		n := copy(p, data)
		if n < len(data) {
			c.readBuf = append(c.readBuf, data[n:]...)
		}
		return n, nil
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.closeMu.Unlock()

	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		c.closed = true
		c.closeMu.Unlock()

		// Send close frame gracefully
		msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		if err := c.ws.WriteControl(websocket.CloseMessage, msg, time.Now().Add(5*time.Second)); err != nil {
			log.Printf("wsConn.Close: WriteControl error: %v", err)
		}
		err = c.ws.Close()
	})
	return err
}

func (c *wsConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1080}
}

func (c *wsConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 1081}
}

func (c *wsConn) SetDeadline(t time.Time) error {
	_ = c.ws.SetReadDeadline(t)
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.ws.SetReadDeadline(t)
}

func (c *wsConn) SetWriteDeadline(t time.Time) error {
	return c.ws.SetWriteDeadline(t)
}
