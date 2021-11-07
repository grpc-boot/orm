package model

type User struct {
	Id        int64  `borm:"id,primary"`
	NickName  string `borm:"nickname"`
	CreatedAt int64  `borm:"created_at,required"`
}

func (u User) TableName() string {
	return `user`
}
