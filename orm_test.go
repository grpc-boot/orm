package orm

import (
	"testing"

	"github.com/grpc-boot/base"
)

func TestCondition_Sql(t *testing.T) {
	con := orCondition(map[string][]interface{}{
		"id":          {13},
		"name LIKE":   {"ma%"},
		"age BETWEEN": {10, 20},
		"month":       {1, 6, 7},
	})

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	t.Log(con.Sql(&args), args)
}

func TestWhere_Sql(t *testing.T) {
	con := orCondition(map[string][]interface{}{
		"`id`":          {13},
		"`name` LIKE":   {"ma%"},
		"`age` BETWEEN": {10, 20},
		"`month`":       {1, 6, 7},
	})

	con1 := orCondition(map[string][]interface{}{
		"`id` >=": {13},
	})

	where := Where{
		con, con1,
	}

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	t.Log(where.Sql(&args), args)
}

func TestMysqlQuery_Sql(t *testing.T) {
	query := NewMysqlQuery()
	defer query.Close()

	query.From("`user`").Select("`id`", "`name`")
	query.Where(map[string][]interface{}{
		"`id`":    {12, 45, 67},
		"`is_on`": {1},
	})

	query.AndWhere(map[string][]interface{}{
		"`id` >": {10},
	})

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	t.Log(query.Sql(&args), args)

	query.Offset(100).Limit(10)
	t.Fatal(query.Sql(&args), args)
}
