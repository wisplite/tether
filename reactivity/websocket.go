package reactivity

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func Handle(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade to websocket", "error", err)
		return
	}
	defer ws.Close()

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			slog.Error("Failed to read message", "error", err)
			return
		}
		slog.Debug("Received message", "message", string(message))
	}
}
