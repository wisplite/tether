package tether

import (
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
	return &Engine{db: db, channels: make(map[string]*reactivity.Channel), mutations: make(map[string]func(ctx *MutationCtx) error), queries: make(map[string]func(ctx *QueryCtx) error)}
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) error) {
	e.mutations[name] = mutation
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) error) {
	e.queries[name] = query
}


