# 目录
<!-- TOC -->
- [orm](#orm)
    - [1.显示所有数据库表，并将表结构转换为golang结构体](#显示所有数据库表)
    - [2.insert语句](#insert语句)
    - [3.select语句](#select语句)

<!-- /TOC -->

# orm

> 数据库及表信息

```text
数据库：
host: 127.0.0.1
port: 3306
userName: root
password: 123456
database: dd

表结构：
CREATE TABLE `gateway` (
  `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '名称',
  `path` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '路径',
  `second_limit` int DEFAULT '5000' COMMENT '每秒请求数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `path` (`path`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

CREATE TABLE `orm` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `lid` bigint(10) unsigned zerofill DEFAULT '0000000000' COMMENT '逻辑ID',
  `name` varchar(32) DEFAULT NULL COMMENT '名字',
  `pwd` char(32) DEFAULT NULL COMMENT '密码',
  `created_at` timestamp(6) NULL DEFAULT NULL COMMENT '创建时间',
  `updated_at` datetime DEFAULT NULL COMMENT '修改时间',
  `remark` text COMMENT '备注',
  `price` decimal(18,2) unsigned zerofill DEFAULT '0000000000000000.00' COMMENT '价格',
  `is_on` bit(19) DEFAULT b'1' COMMENT '是否启用',
  `logo` blob COMMENT '头像',
  `amount` float(12,4) unsigned DEFAULT '0.0000' COMMENT '总量',
  `dfd` bigint unsigned NOT NULL DEFAULT '3' COMMENT '余额',
  `sdf` set('1','2','3','4') NOT NULL DEFAULT '' COMMENT 'set测试',
  `em` enum('1','3','5','7') NOT NULL DEFAULT '1' COMMENT 'enum测试',
  `jn_` json NOT NULL COMMENT 'json测试',
  PRIMARY KEY (`id`),
  KEY `is_on` (`is_on`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

CREATE TABLE `user` (
  `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '用户ID',
  `nickname` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '昵称',
  `created_at` bigint unsigned DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint unsigned DEFAULT '0' COMMENT '更新时间',
  `is_on` tinyint unsigned DEFAULT '0' COMMENT '启用状态',
  PRIMARY KEY (`id`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
```

## 显示所有数据库表

> 代码

```go
package main

import (
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/orm"
)

func main() {
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

	group, err := orm.NewMysqlGroup(groupOption)

	if err != nil {
		base.RedFatal("instance mysql group err:%s", err.Error())
	}

	var tableList []string
	tableList, err = group.Tables("", false)
	if err != nil {
		base.RedFatal("query table err:%s", err.Error())
	}

	base.Green("%+v", tableList)

	var t *orm.Table
	for _, table := range tableList {
		t, err = group.Table(table, false)
		if err != nil {
			base.RedFatal("query table info err:%s", err.Error())
		}
		base.Green(t.ToStruct())
	}
}
```

> 输出

```text
[gateway orm user]
type Gateway struct {
    Id uint64 `json:"id" borm:"id"`
    Name string `json:"name" borm:"name"`
    Path string `json:"path" borm:"path"`
    SecondLimit int64 `json:"second_limit" borm:"second_limit"`
}
type Orm struct {
    Id uint64 `json:"id" borm:"id"`
    Lid uint64 `json:"lid" borm:"lid"`
    Name string `json:"name" borm:"name"`
    Pwd string `json:"pwd" borm:"pwd"`
    CreatedAt string `json:"created_at" borm:"created_at"`
    UpdatedAt string `json:"updated_at" borm:"updated_at"`
    Remark string `json:"remark" borm:"remark"`
    Price float64 `json:"price" borm:"price"`
    IsOn int8 `json:"is_on" borm:"is_on"`
    Logo []byte `json:"logo" borm:"logo"`
    Amount float64 `json:"amount" borm:"amount"`
    Dfd uint64 `json:"dfd" borm:"dfd"`
    Sdf string `json:"sdf" borm:"sdf"`
    Em string `json:"em" borm:"em"`
    Jn string `json:"jn_" borm:"jn_"`
}
type User struct {
    Id uint64 `json:"id" borm:"id"`
    Nickname string `json:"nickname" borm:"nickname"`
    CreatedAt uint64 `json:"created_at" borm:"created_at"`
    UpdatedAt uint64 `json:"updated_at" borm:"updated_at"`
    IsOn uint8 `json:"is_on" borm:"is_on"`
}
```

## insert语句

### insert by orm.Row 

> 代码：

```go
// insert1 INSERT INTO `user`(`nickname`,`created_at`,`is_on`)VALUES(?, ?, ?)
func insert1() {
	res, err := group.Insert("`user`", orm.Row{
		"`nickname`": time.Now().String()[0:22],
		"`created_at`": time.Now().Unix(),
		"`is_on`": 1,
	})
	if err != nil {
		base.RedFatal("insert err:%s", err.Error())
	}

	id, _ := res.LastInsertId()

	base.Green("insert id:%d", id)
}
```

> 输出：

```text
insert id:1
```

### insert by object

```go
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

func insert2() {
	u := User{
		NickName:  fmt.Sprintf("orm_%s", time.Now().String()[0:22]),
		IsOn:      uint8(rand.Intn(1)),
	}

	res, err := group.InsertObj(&u)
	if err != nil {
		base.RedFatal("insert err:%s", err.Error())
	}

	id, _ := res.LastInsertId()

	base.Green("insert id:%d", id)
}
```

## select语句

### 简单查询

> 代码

```go
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
```

> 输出

```text
sql: SELECT * FROM `user` LIMIT 0,10 
select 1: [{"is_on":"0","id":"1","nickname":"m_2022-01-08 22:22:29","created_at":"1641651749","updated_at":"0"},{"id":"3","nickname":"2022-01-09 17:05:14.07","created_at":"1641719114","updated_at":"0","is_on":"1"},{"id":"4","nickname":"2022-01-09 17:05:21.03","created_at":"1641719121","updated_at":"0","is_on":"1"},{"is_on":"1","id":"5","nickname":"2022-01-09 17:05:24.97","created_at":"1641719124","updated_at":"0"},{"created_at":"1641719144","updated_at":"0","is_on":"1","id":"6","nickname":"2022-01-09 17:05:44.13"},{"created_at":"1641719449","updated_at":"0","is_on":"1","id":"7","nickname":"2022-01-09 17:10:49.81"},{"id":"8","nickname":"2022-01-09 17:11:20.00","created_at":"1641719480","updated_at":"0","is_on":"1"},{"updated_at":"0","is_on":"1","id":"9","nickname":"2022-01-09 17:11:45.41","created_at":"1641719505"},{"created_at":"1641719578","updated_at":"0","is_on":"1","id":"10","nickname":"2022-01-09 17:12:58.31"},{"id":"11","nickname":"2022-01-09 17:24:21.37","created_at":"1641720261","updated_at":"0","is_on":"1"}]
```

### 带Where条件

> 代码

```go
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
```

> 输出

```text
sql: SELECT `id`,`nickname`,`is_on` FROM `user` WHERE (`is_on` = ?) ORDER BY `id` DESC,`created_at` DESC LIMIT 0,10 
select 2: [{"is_on":"1","id":"15","nickname":"2022-01-09 17:27:28.79"},{"id":"14","nickname":"2022-01-09 17:26:23.29","is_on":"1"},{"id":"13","nickname":"2022-01-09 17:26:12.72","is_on":"1"},{"is_on":"1","id":"12","nickname":"2022-01-09 17:24:42.33"},{"id":"11","nickname":"2022-01-09 17:24:21.37","is_on":"1"},{"id":"10","nickname":"2022-01-09 17:12:58.31","is_on":"1"},{"id":"9","nickname":"2022-01-09 17:11:45.41","is_on":"1"},{"id":"8","nickname":"2022-01-09 17:11:20.00","is_on":"1"},{"id":"7","nickname":"2022-01-09 17:10:49.81","is_on":"1"},{"nickname":"2022-01-09 17:05:44.13","is_on":"1","id":"6"}]
```

### 复杂Where条件

> 代码

```go
func select3() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.Select("`id`", "`nickname`", "`is_on`").
		From("`user`").
		Where(orm.AndWhere(orm.FieldMap{
			"`is_on`": {1},
		})).
		Or(orm.AndCondition(orm.FieldMap{
			"`id`":{"IN", 1, 2, 3, 4, 5, 6},
			"`nickname`":{"LIKE", "2022%"},
	    })).
		And(orm.OrCondition(orm.FieldMap{
			"created_at":{"<=", time.Now().Unix()},
			"updated_at":{0},
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
```

> 输出

```text
sql: SELECT `id`,`nickname`,`is_on` FROM `user` WHERE (`is_on` = ?) OR (`id` IN(?,?,?,?,?,?) AND `nickname` LIKE ?) AND (created_at <= ? OR updated_at = ?) ORDER BY `id` DESC,`created_at` DESC LIMIT 0,10 
select 3: [{"id":"15","nickname":"2022-01-09 17:27:28.79","is_on":"1"},{"id":"14","nickname":"2022-01-09 17:26:23.29","is_on":"1"},{"is_on":"1","id":"13","nickname":"2022-01-09 17:26:12.72"},{"id":"12","nickname":"2022-01-09 17:24:42.33","is_on":"1"},{"nickname":"2022-01-09 17:24:21.37","is_on":"1","id":"11"},{"id":"10","nickname":"2022-01-09 17:12:58.31","is_on":"1"},{"id":"9","nickname":"2022-01-09 17:11:45.41","is_on":"1"},{"nickname":"2022-01-09 17:11:20.00","is_on":"1","id":"8"},{"id":"7","nickname":"2022-01-09 17:10:49.81","is_on":"1"},{"id":"6","nickname":"2022-01-09 17:05:44.13","is_on":"1"}]
```



