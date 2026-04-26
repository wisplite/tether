package tether

import "gorm.io/gorm"

type AuthCtx struct {
	UserID     string
	IsLoggedIn bool
}

type QueryCtx struct {
	DB      *gorm.DB
	AuthCtx *AuthCtx
	Params  map[string]interface{}
}

type MutationCtx struct {
	DB      *gorm.DB
	AuthCtx *AuthCtx
	Params  map[string]interface{}
}

type Auth interface {
	GetUserID(token string) (string, error)
}
