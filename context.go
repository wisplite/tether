package tether

import "gorm.io/gorm"

type AuthCtx struct {
	UserID     string
	IsLoggedIn bool
}

type QueryCtx struct {
	DB      *gorm.DB
	AuthCtx *AuthCtx
}

type MutationCtx struct {
	DB      *gorm.DB
	AuthCtx *AuthCtx
}
