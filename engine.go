package tether

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/wisplite/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db           *gorm.DB
	dbType       string // sqlite or postgres
	mutations    map[string]func(ctx *MutationCtx) error
	queries      map[string]func(ctx *QueryCtx) error
	dependencies map[string][]string
	tracker      *reactivity.Tracker
}

func NewEngine(db *gorm.DB, dbType string) *Engine {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	tracker := reactivity.NewTracker()
	if dbType != "sqlite" && dbType != "postgres" {
		panic("Invalid database type")
	}
	e := &Engine{db: db, dbType: dbType, mutations: make(map[string]func(ctx *MutationCtx) error), queries: make(map[string]func(ctx *QueryCtx) error), dependencies: make(map[string][]string), tracker: tracker}
	db.Callback().Create().After("gorm:create").Register("tether:after_create", func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		e.InvalidateTable(tx.Statement.Table)
	})
	return e
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) error) {
	e.mutations[name] = mutation // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) error, dependencies []string) {
	e.queries[name] = query // stores the query in the list of valid queries
	for _, dependency := range dependencies {
		e.dependencies[dependency] = append(e.dependencies[dependency], name)
	}
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

func (e *Engine) GetDependentQueries(tableName string) []string {
	return e.dependencies[tableName]
}

func (e *Engine) InvalidateTable(tableName string) {
	slog.Debug("Invalidating table", "table", tableName)
	dependentQueries := e.GetDependentQueries(tableName)
	for _, query := range dependentQueries {
		slog.Debug("Invalidating query", "query", query)
		subscriptions := e.tracker.GetQuerySubscriptions(query)
		for _, subscription := range subscriptions {
			slog.Debug("Invalidating subscription", "subscription", subscription["clientID"])
			params := map[string]interface{}{}
			json.Unmarshal([]byte(subscription["params"]), &params)
			_, err := e.ExecuteQuery(query, params, subscription["clientID"])
			if err != nil {
				slog.Error("Failed to execute query", "error", err)
				continue
			}
		}
	}
}

func (e *Engine) ExecuteQuery(query string, params map[string]interface{}, clientID string) (interface{}, error) {
	/*
		TODO: implement the logic to execute the query
		Steps needed:
		1. Check which tables updated ✅
		2. Get the queries that rely on the tables ✅
		3. Get the subscriptions that need updating ✅
		4. Calculate hash for every query
		5. Send the updated queries if hash changed
	*/
	slog.Debug("Executing query", "query", query, "params", params)
	e.queries[query](&QueryCtx{DB: e.db, AuthCtx: &AuthCtx{UserID: "", IsLoggedIn: true}, Params: params})
	return nil, nil
}

func (e *Engine) ExecuteMutation(mutation string, params map[string]interface{}, clientID string) (interface{}, error) {
	result := e.mutations[mutation](&MutationCtx{DB: e.db, AuthCtx: &AuthCtx{UserID: "", IsLoggedIn: true}, Params: params})
	return result, nil
}

func (e *Engine) OnReceiveMessage(clientID string, msg map[string]interface{}) error {
	slog.Debug("Received message", "from", clientID, "message", msg)
	switch msg["type"] {
	case "query":
		e.tracker.SubscribeToQuery(clientID, msg["location"].(string), msg["params"].(map[string]string))
	case "mutation":
		e.ExecuteMutation(msg["location"].(string), msg["params"].(map[string]interface{}), clientID)
	}
	return nil
}
