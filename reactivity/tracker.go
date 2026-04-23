package reactivity

import (
	"log/slog"
	"sync"
)

// TODO: populate with needed structures for tracking state
type Tracker struct {
	mu sync.RWMutex

	// Maps a Client's UUID to their actual Client struct
	clients map[string]*Client

	// Maps a Query Hash (e.g. "getUser?id=1") to a Set of Client IDs
	subscriptions map[string][]map[string]string
}

func NewTracker() *Tracker {
	return &Tracker{clients: make(map[string]*Client), subscriptions: make(map[string][]map[string]string)}
}

func (t *Tracker) Track(c *Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clients[c.ID] = c
}

func (t *Tracker) Untrack(c *Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.clients, c.ID)
}

func (t *Tracker) SubscribeToQuery(clientID string, query string, params string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subscriptions[query] == nil {
		t.subscriptions[query] = make([]map[string]string, 0)
	}
	// set t.subscriptions[query] to a map of client IDs and their params
	t.subscriptions[query] = append(t.subscriptions[query], map[string]string{"clientID": clientID, "params": params})
	slog.Debug("Tracker: Subscribed to query", "query", query, "clientID", clientID, "params", params)
}

func (t *Tracker) UnsubscribeFromQuery(clientID string, query string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, subscription := range t.subscriptions[query] {
		if subscription["clientID"] == clientID {
			t.subscriptions[query] = append(t.subscriptions[query][:i], t.subscriptions[query][i+1:]...)
			break
		}
	}
}

func (t *Tracker) GetQuerySubscriptions(query string) []map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	subscriptions := t.subscriptions[query]
	return subscriptions
}

func (t *Tracker) SendMessage(clientID string, message []byte) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	client := t.clients[clientID]
	if client == nil {
		slog.Error("Tracker: Client not found", "clientID", clientID)
		return
	}
	select {
	case client.Send <- message:
	default:
		slog.Error("Tracker: Client send channel is full", "clientID", clientID)
	}
}
