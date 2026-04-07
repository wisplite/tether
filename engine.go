package tether

import (
	"log/slog"
	"net/http"

	"github.com/wisplite/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db        *gorm.DB
	channels  map[string]*reactivity.Channel
	mutations map[string]func(ctx *MutationCtx) error
	queries   map[string]func(ctx *QueryCtx) error
}

func NewEngine(db *gorm.DB) *Engine {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	return &Engine{db: db, channels: make(map[string]*reactivity.Channel), mutations: make(map[string]func(ctx *MutationCtx) error), queries: make(map[string]func(ctx *QueryCtx) error)}
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) error) {
	e.mutations[name] = mutation
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) error) {
	e.queries[name] = query
	slog.Debug("Registered query", "name", name)
}

func (e *Engine) CreateTable(name string, schema interface{}) {
	e.db.AutoMigrate(schema)
	slog.Debug("Created table", "name", name)
}

func (e *Engine) Handle(w http.ResponseWriter, r *http.Request) {
	reactivity.Handle(w, r)
}
