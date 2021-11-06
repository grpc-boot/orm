package orm

// go test . -v
// go test -bench=. -benchmem -benchtime=5s
import (
	"testing"
	"time"

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

	args = args[:0]
	t.Log(query.Sql(&args), args)
}

func TestInsert(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := Insert(&args, "`user`", map[string]interface{}{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	}, map[string]interface{}{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	})
	t.Log(sql, args)
}

func TestUpdateAll(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := UpdateAll(&args, "`user`", map[string]interface{}{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	}, Where{
		andCondition(map[string][]interface{}{
			"`id`": {1, 3, 5},
		}),
		orCondition(map[string][]interface{}{
			"`status`": {1},
		}),
	})

	t.Log(sql, args)
}

func TestDeleteAll(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := DeleteAll(&args, "`user`", Where{
		andCondition(map[string][]interface{}{
			"`id`": {1, 3, 5},
		}),
		orCondition(map[string][]interface{}{
			"`status`": {1},
		}),
	})

	t.Log(sql, args)
}

// BenchmarkMysqlQuery_Sql-4         951957              1088 ns/op            1000 B/op         20 allocs/op
func BenchmarkMysqlQuery_Sql(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			query := NewMysqlQuery()
			args := base.AcquireArgs()

			query.From("`user`").Where(map[string][]interface{}{
				"`id`":        {1, 5, 10, 30, 23, 56},
				"`name` LIKE": {"dd%"},
				"`status` >":  {0},
			})

			query.Sql(&args)

			base.ReleaseArgs(&args)
			query.Close()
		}
	})
}

func BenchmarkMysqlQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		query := NewMysqlQuery()
		args := base.AcquireArgs()

		query.From("`user`").Where(map[string][]interface{}{
			"`id`":        {1, 5, 10, 30, 23, 56},
			"`name` LIKE": {"dd%"},
			"`status` >":  {0},
		})

		query.Sql(&args)

		base.ReleaseArgs(&args)
		query.Close()
	}
}
