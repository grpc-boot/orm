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

> 代码：

```text
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

## select语句

### 简单查询

> 代码

```text
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
```

### 带Where条件

> 代码

```text
// select2 SELECT `id`, `nickname`, `is_on` FROM `user` WHERE `is_on`=1 ORDER BY `id` DESC,`created_at` DESC LIMIT 10
func select2() {
	query := orm.AcquireQuery4Mysql()
	defer query.Close()

	query.Select("`id`", "`nickname`", "`is_on`").
		From("`user`").
		Where(orm.AndWhere(orm.FieldMap{
			"`is_on`":{ 1 },
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
```


