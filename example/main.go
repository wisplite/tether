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

	engine := tether.NewEngine(db, "sqlite")

	engine.CreateTable("users", &User{})
	engine.CreateTable("messages", &Messages{})

	engine.RegisterQuery("getMessages", func(ctx *tether.QueryCtx) interface{} {
		var messages []Messages
		ctx.DB.Where("room_id = ?", ctx.Params["room"].(string)).Find(&messages)
		return messages
	}, []string{"messages"})

	engine.RegisterMutation("createMessage", func(ctx *tether.MutationCtx) interface{} {
		msg := &Messages{ID: uuid.NewString(), Message: ctx.Params["message"].(string), SenderID: ctx.AuthCtx.UserID, RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
	})

	http.HandleFunc("/tether", engine.Handle)
	http.ListenAndServe(":8080", nil)
}
