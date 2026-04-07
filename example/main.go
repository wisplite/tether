package main

import (
	"github.com/glebarez/sqlite"
	"github.com/wisplite/tether"
	"gorm.io/gorm"
)

func main() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	engine := tether.NewEngine(db)

	engine.RegisterMutation("createUser", func(ctx *tether.MutationCtx) error {
		return nil
	})

	engine.RegisterQuery("getUser", func(ctx *tether.QueryCtx) error {
		return nil
	})
}
