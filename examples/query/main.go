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

	for _, table := range tableList {
		t, err := group.Table(table, false)
		if err != nil {
			base.RedFatal("query table info err:%s", err.Error())
		}
		base.Green(t.ToStruct())
	}
}
