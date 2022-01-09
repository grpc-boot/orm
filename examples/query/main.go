package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/orm"

	jsoniter "github.com/json-iterator/go"
)

var (
	group orm.Group
)

func init() {
	groupOption := &orm.GroupOption{
		RetryInterval: 60,
		Masters: []orm.PoolOption{
			{
				Dsn:             "root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s",
				MaxConnLifetime: 600,
				MaxOpenConns:    50,
				MaxIdleConns:    10,
			},
		},
	}

	var err error
	group, err = orm.NewMysqlGroup(groupOption)

	if err != nil {
		base.RedFatal("instance mysql group err:%s", err.Error())
	}
}

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

// BeforeCreate insert时候执行
func (u *User) BeforeCreate() {
	u.CreatedAt = time.Now().Unix()
}

// BeforeSave insert与update均会执行
func (u *User) BeforeSave() {
	u.UpdatedAt = time.Now().Unix()
}

func main() {
	insert1()
	insert2()
	select1()
	select2()
	select3()
}

func insert1() {
	res, err := group.Insert("`user`", orm.Row{
		"`nickname`":   time.Now().String()[0:22],
		"`created_at`": time.Now().Unix(),
		"`is_on`":      1,
	})
	if err != nil {
		base.RedFatal("insert err:%s", err.Error())
	}

	id, _ := res.LastInsertId()

	base.Green("insert id:%d", id)
}

func insert2() {
	u := User{
		NickName: fmt.Sprintf("orm_%s", time.Now().String()[0:22]),
		IsOn:     uint8(rand.Intn(1)),
	}

	res, err := group.InsertObj(&u)
	if err != nil {
		base.RedFatal("insert err:%s", err.Error())
	}

	id, _ := res.LastInsertId()

	base.Green("insert id:%d", id)
}

func select1() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.From("`user`").Limit(10)
	rows, err := group.Find(query, false)
	if err != nil {
		base.RedFatal("query err:%s", err.Error())
	}

	var (
		d, _ = jsoniter.Marshal(rows)
		args = []interface{}{}
	)

	base.Green("sql: %s \nselect 1: %s", query.Sql(&args), d)
}

func select2() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.Select("`id`", "`nickname`", "`is_on`").
		From("`user`").
		Where(orm.AndWhere(orm.FieldMap{
			"`is_on`": {1},
		})).
		Order("`id` DESC", "`created_at` DESC").
		Limit(10)

	rows, err := group.Find(query, true)
	if err != nil {
		base.RedFatal("query err:%s", err.Error())
	}

	var (
		d, _ = jsoniter.Marshal(rows)
		args = []interface{}{}
	)

	base.Green("sql: %s \nselect 2: %s", query.Sql(&args), d)
}

func select3() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.Select("`id`", "`nickname`", "`is_on`").
		From("`user`").
		Where(orm.AndWhere(orm.FieldMap{
			"`is_on`": {1},
		})).
		Or(orm.AndCondition(orm.FieldMap{
			"`id`":       {"IN", 1, 2, 3, 4, 5, 6},
			"`nickname`": {"LIKE", "2022%"},
		})).
		And(orm.OrCondition(orm.FieldMap{
			"created_at": {"<=", time.Now().Unix()},
			"updated_at": {0},
		})).
		Order("`id` DESC", "`created_at` DESC").
		Limit(10)

	rows, err := group.Find(query, true)
	if err != nil {
		base.RedFatal("query err:%s", err.Error())
	}

	var (
		d, _ = jsoniter.Marshal(rows)
		args = []interface{}{}
	)

	base.Green("sql: %s \nselect 3: %s", query.Sql(&args), d)
}
