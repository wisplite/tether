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
	subscriptions map[string]map[string]bool
}

func NewTracker() *Tracker {
	return &Tracker{clients: make(map[string]*Client), subscriptions: make(map[string]map[string]bool)}
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

func (t *Tracker) SubscribeToQuery(clientID string, queryHash string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subscriptions[queryHash] == nil {
		t.subscriptions[queryHash] = make(map[string]bool)
	}
	t.subscriptions[queryHash][clientID] = true
}

func (t *Tracker) UnsubscribeFromQuery(clientID string, queryHash string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.subscriptions[queryHash], clientID)
}

func (t *Tracker) GetQuerySubscriptions(queryHash string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	subscriptions := t.subscriptions[queryHash]
	subscriptionIDs := make([]string, 0, len(subscriptions))
	for clientID := range subscriptions {
		subscriptionIDs = append(subscriptionIDs, clientID)
	}
	return subscriptionIDs
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
