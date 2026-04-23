package reactivity

import (
	"log/slog"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	ID     string
	Conn   *websocket.Conn
	Send   chan []byte
	AuthID string
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{ID: uuid.NewString(), Conn: conn, Send: make(chan []byte, 256), AuthID: ""}
}

func (c *Client) SetAuthID(authID string) {
	c.AuthID = authID
}

func (c *Client) GetAuthID() string {
	return c.AuthID
}

func (c *Client) WritePump() {
	defer c.Conn.Close()

	for message := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			slog.Error("Client write pump failed", "error", err, "client", c.ID)
			return
		}
	}
}
