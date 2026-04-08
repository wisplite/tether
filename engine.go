package tether

import (
	"log/slog"
	"net/http"

	"github.com/wisplite/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db        *gorm.DB
	mutations map[string]func(ctx *MutationCtx) error
	queries   map[string]func(ctx *QueryCtx) error
	tracker   *reactivity.Tracker
}

func NewEngine(db *gorm.DB) *Engine {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	tracker := reactivity.NewTracker()
	return &Engine{db: db, mutations: make(map[string]func(ctx *MutationCtx) error), queries: make(map[string]func(ctx *QueryCtx) error), tracker: tracker}
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) error) {
	e.mutations[name] = mutation // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) error) {
	e.queries[name] = query // stores the query in the list of valid queries
	slog.Debug("Registered query", "name", name)
}

func (e *Engine) CreateTable(name string, schema interface{}) {
	e.db.AutoMigrate(schema)
	slog.Debug("Created table", "name", name)
}

func (e *Engine) Handle(w http.ResponseWriter, r *http.Request) {
	reactivity.Handle(w, r, e, e.tracker) // wraps the raw websocket connection with the engine handler
}

func (e *Engine) OnConnect(clientID string) error {
	slog.Debug("Connected to websocket", "client", clientID)
	// TODO: implement the logic to handle the connection
	return nil
}

func (e *Engine) OnDisconnect(clientID string) error {
	slog.Debug("Disconnected from websocket", "client", clientID)
	// TODO: implement the logic to handle the disconnection
	return nil
}

func (e *Engine) OnReceiveMessage(clientID string, msg map[string]interface{}) error {
	slog.Debug("Received message", "from", clientID, "message", msg)
	// TODO: implement the logic to handle the message
	return nil
}
