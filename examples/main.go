package main

import (
	"os"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/orm"
	"github.com/grpc-boot/orm/examples/model"
)

/**
CREATE TABLE `user` (
`id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '用户ID',
`nickname` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '昵称',
`created_at` bigint unsigned DEFAULT NULL COMMENT '创建时间',
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/

type Config struct {
	Db orm.GroupOption `yaml:"db" json:"db"`
}

var (
	group orm.Group
)

func main() {
	conf := &Config{}

	err := base.YamlDecodeFile("app.yml", conf)

	if err != nil {
		base.Red("init group err:%s", err.Error())
		os.Exit(1)
	}

	base.Green("use config:%v", conf)

	group, err = orm.NewMysqlGroup(&conf.Db)
	if err != nil {
		base.RedFatal("init group err:%s", err.Error())
	}

	insert()

	update()

	deleteObj()

	query()
}

func insert() {
	current := time.Now()
	result, err := group.InsertObj(model.User{
		NickName:  current.Format(`2006-01-02 15:04:05`),
		CreatedAt: current.Unix(),
	})

	if err != nil {
		base.RedFatal("insert obj err:%s", err.Error())
	}

	id, err := result.LastInsertId()
	if err != nil {
		base.RedFatal("get insertId err:%s", err.Error())
	}

	base.Green("insert id:%d", id)
}

func update() {
	result, err := group.UpdateObj(model.User{
		Id:        1,
		NickName:  "update nickName",
		CreatedAt: time.Now().Unix(),
	})

	if err != nil {
		base.RedFatal("update err:%s", err.Error())
	}

	rows, _ := result.RowsAffected()
	base.Green("update rows %d", rows)
}

func deleteObj() {
	result, err := group.DeleteObj(model.User{Id: 1})
	if err != nil {
		base.RedFatal("delete err:%s", err.Error())
	}

	rows, _ := result.RowsAffected()
	base.Green("delete rows %d", rows)
}

func query() {
	q := orm.NewMysqlQuery()
	q.From("`user`").Where(orm.AndCondition(map[string][]interface{}{
		"`created_at`": {">", 0},
	})).Limit(1)

	rows, err := group.Find(q, false)
	if err != nil {
		base.RedFatal("query one err:%s", err.Error())
	}

	user := &model.User{}
	err = orm.ToObj(rows, user)
	if err != nil {
		base.RedFatal("query one err:%s", err.Error())
	}

	base.Green("%v", user)

	rows, err = group.Find(q.Limit(10), false)
	if err != nil {
		base.RedFatal("query all err:%s", err.Error())
	}

	userList, err := orm.ToMap(rows)
	if err != nil {
		base.RedFatal("all to map err:%s", err.Error())
	}
	base.Green("%v", userList)
}
