package reactivity

import "github.com/gorilla/websocket"

// TODO: populate with needed structures for tracking state
type Tracker struct {
}

func NewTracker() *Tracker {
	return &Tracker{}
}

func (t *Tracker) Track(conn *websocket.Conn) {}

func (t *Tracker) Untrack(conn *websocket.Conn) {}

func (t *Tracker) SubscribeToQuery(query string) {}

func (t *Tracker) UnsubscribeFromQuery(query string) {}

func (t *Tracker) GetQuerySubscriptions() []string {
	return []string{}
}
