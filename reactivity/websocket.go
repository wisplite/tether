package reactivity

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

// MessageReceiver receives decoded WebSocket payloads. Implemented by tether.Engine.
type MessageReceiver interface {
	OnReceiveMessage(msg map[string]interface{}) error
}

var upgrader = websocket.Upgrader{}

func Handle(w http.ResponseWriter, r *http.Request, e MessageReceiver) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WS: Failed to upgrade to websocket", "error", err)
		return
	}
	defer ws.Close()

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			slog.Error("WS: Failed to read message", "error", err)
			return
		}
		slog.Debug("WS: Received message", "message", string(message))

		var msg map[string]interface{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			slog.Error("WS: Failed to unmarshal message", "error", err)
			return
		}
		slog.Debug("WS: Unmarshalled message", "message", msg)
		err = e.OnReceiveMessage(msg)
		if err != nil {
			slog.Error("WS: Failed to on receive message", "error", err)
			return
		}
	}
}
