package main

import (
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

func main() {
	insert1()
	select1()
	select2()
}

// insert1 INSERT INTO `user`(`nickname`,`created_at`,`is_on`)VALUES(?, ?, ?)
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

// select1 SELECT * FROM `user` LIMIT 10
func select1() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.From("`user`").Limit(10)
	rows, err := group.Find(query, false)
	if err != nil {
		base.RedFatal("query err:%s", err.Error())
	}

	d, _ := jsoniter.Marshal(rows)
	base.Green("select 1: %s", d)
}

// select2 SELECT `id`, `nickname`, `is_on` FROM `user` WHERE `is_on`=1 ORDER BY `id` DESC,`created_at` DESC LIMIT 10
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

	d, _ := jsoniter.Marshal(rows)
	base.Green("select 2: %s", d)
}
