package model

import (
	"time"
)

type User struct {
	Id        int64  `borm:"id,primary"`
	NickName  string `borm:"nickname"`
	IsOn      uint8  `borm:"is_on,required"`
	CreatedAt int64  `borm:"created_at"`
	UpdatedAt int64  `borm:"updated_at"`
}

func (u *User) TableName() string {
	return `user`
}

func (u *User) BeforeCreate() {
	u.CreatedAt = time.Now().Unix()
}

func (u *User) BeforeSave() {
	u.UpdatedAt = time.Now().Unix()
}
