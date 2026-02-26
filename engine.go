package tether

import (
	"github.com/wisplite/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db       *gorm.DB
	channels map[string]*reactivity.Channel
}

func NewEngine(db *gorm.DB) *Engine {
	return &Engine{db: db, channels: make(map[string]*reactivity.Channel)}
}
