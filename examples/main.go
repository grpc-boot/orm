package main

import (
	"fmt"
	"math/rand"
	"strconv"
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
  `updated_at` bigint unsigned DEFAULT '0' COMMENT '更新时间',
  `is_on` tinyint unsigned DEFAULT '0' COMMENT '启用状态',
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
	rand.Seed(time.Now().UnixNano())

	conf := &Config{}

	err := base.YamlDecodeFile("app.yml", conf)

	if err != nil {
		base.RedFatal("init group err:%s", err.Error())
	}

	base.Green("use config:%v", conf)

	group, err = orm.NewMysqlGroup(&conf.Db)
	if err != nil {
		base.RedFatal("init group err:%s", err.Error())
	}

	id := insert()

	lastInsertUser := findOneObj(id)

	base.Green("lastInsert User: %v", lastInsertUser)

	update(lastInsertUser)

	find()

	deleteObj(lastInsertUser)

	groupSql()

	query()

	//deleteAll()
}

func insert() int64 {
	current := time.Now()

	r, err := group.Insert(`user`, map[string]interface{}{
		"nickname":   fmt.Sprintf("m_%s", current.Format(`2006-01-02 15:04:05`)),
		"created_at": current.Unix(),
	})

	if err != nil {
		base.RedFatal("insert err:%s", err.Error())
	}

	id, err := r.LastInsertId()
	if err != nil {
		base.RedFatal("get insertId err:%s", err.Error())
	}
	base.Green("insert id:%d", id)

	result, err := group.InsertObj(&model.User{
		NickName: current.Format(`2006-01-02 15:04:05`),
	})

	if err != nil {
		base.RedFatal("insert obj err:%s", err.Error())
	}

	id, err = result.LastInsertId()
	if err != nil {
		base.RedFatal("get insertId err:%s", err.Error())
	}

	base.Green("insert id:%d", id)

	return id
}

func findOneObj(id int64) *model.User {
	var (
		user      = &model.User{}
		condition = orm.AndCondition(map[string][]interface{}{
			"id": {id},
		})
	)

	u, err := group.FindOne(user.TableName(), orm.NewWhere(condition), false)
	if err != nil {
		base.RedFatal("find one err:%s", err.Error())
	}
	base.Green("find one user:%v", u)

	err = group.FindOneObj(orm.NewWhere(condition), user, true)

	if err != nil {
		base.RedFatal("find one obj err:%s", err.Error())
	}
	return user
}

func update(user *model.User) {
	user.NickName = "update_" + strconv.FormatInt(rand.Int63(), 10)
	user.IsOn = 1

	result, err := group.UpdateAll(user.TableName(), map[string]interface{}{
		"nickname": user.NickName,
	}, orm.AndWhere(orm.FieldMap{
		"id": {user.Id},
	}))

	if err != nil {
		base.RedFatal("update all err:%s", err.Error())
	}

	rows, _ := result.RowsAffected()
	base.Green("update [%d] affected rows %d", user.Id, rows)

	result, err = group.UpdateObj(user)

	if err != nil {
		base.RedFatal("update [%d] err:%s", user.Id, err.Error())
	}

	rows, _ = result.RowsAffected()
	base.Green("update [%d] affected rows %d", user.Id, rows)

	base.Green("after update user:%v", user)
}

func deleteObj(user *model.User) {
	result, err := group.DeleteObj(user)
	if err != nil {
		base.RedFatal("delete err:%s", err.Error())
	}

	rows, _ := result.RowsAffected()
	base.Green("delete [%d] rows %d", user.Id, rows)
}

func find() {
	q := orm.AcquireQuery4Mysql()
	defer q.Close()

	user := &model.User{}
	q.From(user.TableName())

	userList, err := group.FindAll(q, user, false)
	if err != nil {
		base.RedFatal("find err:%s", err.Error())
	}

	if userList != nil {
		for _, u := range userList {
			base.Green("user List:%v", u)
		}
	}
}

func groupSql() {
	users, err := group.Query(true, `SELECT * FROM user LIMIT 10`)
	if err != nil {
		base.RedFatal("sql query err:%s", err.Error())
	}

	base.Green("sql query:%v", users)

	r, err := group.Exec(`UPDATE user SET updated_at= 1 WHERE is_on>0`)
	if err != nil {
		base.RedFatal("sql exec err:%s", err.Error())
	}

	rows, err := r.RowsAffected()
	if err != nil {
		base.RedFatal("get rows affected err:%s", err.Error())
	}
	base.Green("rows affected :%d", rows)
}

func query() {
	q := orm.AcquireQuery4Mysql()
	defer q.Close()

	q.Select("id", "is_on").From(`user`).Order("`id` DESC").Offset(1).Limit(10)

	rows, err := group.Find(q, false)
	if err != nil {
		base.RedFatal("query err:%s", err.Error())
	}

	base.Green("query find: %v", rows)
}

func deleteAll() {
	r, err := group.DeleteAll(`user`, orm.AndWhere(orm.FieldMap{
		"id": {">", 0},
	}))
	if err != nil {
		base.RedFatal("delete all err:%s", err.Error())
	}

	rows, err := r.RowsAffected()
	if err != nil {
		base.RedFatal("get rows affected err:%s", err.Error())
	}
	base.Green("rows affected :%d", rows)
}
