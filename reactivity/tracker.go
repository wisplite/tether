package reactivity

import (
	"log/slog"
	"slices"
	"sync"
)

// TODO: populate with needed structures for tracking state
type Tracker struct {
	mu sync.RWMutex

	// Maps a Client's UUID to their actual Client struct
	clients map[string]*Client

	// Maps a Query Hash (e.g. "getUser?id=1") to a Set of Client IDs
	subscriptions       map[string][]map[string]string
	clientSubscriptions map[string][]string
}

func NewTracker() *Tracker {
	return &Tracker{
		clients:             make(map[string]*Client),
		subscriptions:       make(map[string][]map[string]string),
		clientSubscriptions: make(map[string][]string),
	}
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
	delete(t.clientSubscriptions, c.ID)
	for query, subs := range t.subscriptions {
		kept := subs[:0]
		for _, sub := range subs {
			if sub["clientID"] != c.ID {
				kept = append(kept, sub)
			}
		}
		if len(kept) == 0 {
			delete(t.subscriptions, query)
		} else {
			t.subscriptions[query] = kept
		}
	}
}

func (t *Tracker) SetAuthID(clientID string, authID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clients[clientID].SetAuthID(authID)
}

func (t *Tracker) GetAuthID(clientID string) string {
	if _, ok := t.clients[clientID]; !ok {
		slog.Error("Tracker: Client not found", "clientID", clientID)
		return ""
	}
	return t.clients[clientID].GetAuthID()
}

func (t *Tracker) SubscribeToQuery(clientID string, query string, params string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subscriptions[query] == nil {
		t.subscriptions[query] = make([]map[string]string, 0)
	}
	if t.clientSubscriptions[clientID] == nil {
		t.clientSubscriptions[clientID] = make([]string, 0)
	}
	if slices.Contains(t.clientSubscriptions[clientID], query) {
		return // avoid duplicate subscriptions
	}
	t.clientSubscriptions[clientID] = append(t.clientSubscriptions[clientID], query)
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
