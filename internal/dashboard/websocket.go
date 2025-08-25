package dashboard

import (
    "net/http"
    "github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool {
        return true
    },
}

type WebSocketServer struct {
    clients   map[*websocket.Conn]bool
    broadcast chan []byte
}

func NewWebSocketServer() *WebSocketServer {
    return &WebSocketServer{
        clients:   make(map[*websocket.Conn]bool),
        broadcast: make(chan []byte),
    }
}

func (s *WebSocketServer) HandleConnections(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        return
    }
    defer conn.Close()
    s.clients[conn] = true

    for {
        _, message, err := conn.ReadMessage()
        if err != nil {
            delete(s.clients, conn)
            break
        }
        s.broadcast <- message
    }
}

func (s *WebSocketServer) HandleMessages() {
    for {
        message := <-s.broadcast
        for client := range s.clients {
            err := client.WriteMessage(websocket.TextMessage, message)
            if err != nil {
                client.Close()
                delete(s.clients, client)
            }
        }
    }
}