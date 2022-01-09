# 目录
<!-- TOC -->
- [orm](#orm)
    - [1.实例化](#实例化)
    - [2.Option解析](#Option解析)
    - [3.在gin中使用](#在gin中使用)
    - [4.用redis做options配置存储](#用redis做options配置存储)
    - [5.用mysql做options配置存储](#用mysql做options配置存储)

<!-- /TOC -->

# orm

## 显示所有数据库表，并将表结构转换为golang结构体

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



