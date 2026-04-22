package main

import (
	"time"

	"net/http"

	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"github.com/wisplite/tether"
	"gorm.io/gorm"
)

type User struct {
	ID   string `gorm:"primaryKey"`
	Name string
}

type Messages struct {
	ID        string `gorm:"primaryKey"`
	Message   string
	SenderID  string
	RoomID    string
	CreatedAt time.Time
}

func main() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	engine := tether.NewEngine(db)

	engine.CreateTable("users", &User{})
	engine.CreateTable("messages", &Messages{})

	engine.RegisterMutation("createUser", func(ctx *tether.MutationCtx) error {
		return nil
	})

	engine.RegisterQuery("getUser", func(ctx *tether.QueryCtx) error {
		return nil
	})

	engine.RegisterMutation("createMessage", func(ctx *tether.MutationCtx) error {
		ctx.DB.Create(&Messages{ID: uuid.NewString(), Message: ctx.Params["message"].(string), SenderID: ctx.AuthCtx.UserID, RoomID: ctx.Params["room"].(string)})
		return nil
	})

	http.HandleFunc("/tether", engine.Handle)
	http.ListenAndServe(":8080", nil)
}
