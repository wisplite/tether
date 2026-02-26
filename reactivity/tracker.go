package reactivity

import "github.com/gorilla/websocket"

type Subscription struct {
	ID   string
	Conn *websocket.Conn
}

type Channel struct {
	ID            string
	Subscriptions []*Subscription
}
