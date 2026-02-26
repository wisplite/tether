package tether

import (
	"github.com/wisplite/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db        *gorm.DB
	channels  map[string]*reactivity.Channel
	mutations []func(ctx *MutationCtx) error
	queries   []func(ctx *QueryCtx) error
}

func NewEngine(db *gorm.DB) *Engine {
	return &Engine{db: db, channels: make(map[string]*reactivity.Channel)}
}

func (e *Engine) RegisterMutation(mutation func(ctx *MutationCtx) error) {
	e.mutations = append(e.mutations, mutation)
}
