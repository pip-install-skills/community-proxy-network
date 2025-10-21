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