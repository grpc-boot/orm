package orm

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

// go test . -v
// go test -bench=. -benchmem -benchtime=5s
// brew services start mysql
import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/grpc-boot/base"
)

var (
	g Group
)

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

func (u *User) BeforeCreate() {
	u.CreatedAt = time.Now().Unix()
}

func (u *User) BeforeSave() {
	u.UpdatedAt = time.Now().Unix()
}

func init() {
	var err error
	g, err = NewMysqlGroup(&GroupOption{
		Masters: []PoolOption{
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
		},
		Slaves: []PoolOption{
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
			{
				Dsn:             `root:123456@tcp(127.0.0.1:3306)/dd?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns:    15,
				MaxIdleConns:    5,
			},
		},
		RetryInterval: 60,
	})

	if err != nil {
		log.Fatal(err)
	}
}

func TestCondition_Sql(t *testing.T) {
	cond := FieldMap{
		"id":    {13},
		"name":  {`LIKE`, "ma%"},
		"age":   {`BETWEEN`, 10, 20},
		"month": {`IN`, 1, 6, 7},
	}

	con := OrCondition(cond)

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	t.Log(con.Sql(&args), args)

	args = args[:0]
	con = AndCondition(cond)
	t.Log(con.Sql(&args), args)
}

func TestWhere_Sql(t *testing.T) {
	con := OrCondition(FieldMap{
		"`id`":    {13},
		"`name`":  {`LIKE`, "ma%"},
		"`age`":   {`BETWEEN`, 10, 20},
		"`month`": {`IN`, 1, 6, 7},
	})

	con1 := OrCondition(FieldMap{
		"`id`": {`>=`, 13},
	})

	w := NewWhere(con1).Or(con)

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	t.Log(w.Sql(&args), args)
}

func TestMysqlQuery_Sql(t *testing.T) {
	query := AcquireQuery4Mysql()
	defer query.Close()

	query.From("`user`").Select("`id`", "`name`")
	query.Where(OrWhere(FieldMap{
		"`id`":    {`IN`, 12, 45, 67},
		"`is_on`": {1},
	}))

	query.And(AndCondition(FieldMap{
		"`id`": {`>`, 10},
	}))

	query.Or(AndCondition(FieldMap{
		"`sex`": {1},
	}))

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

	sql := SqlInsert(&args, "`user`", Row{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	}, Row{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	})
	t.Log(sql, args)
}

func TestUpdateAll(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := SqlUpdate(&args, "`user`", Row{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	}, AndWhere(FieldMap{
		"`id`": {`IN`, 1, 3, 5},
	}))

	t.Log(sql, args)
}

func TestDeleteAll(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := SqlDelete(&args, "`user`", AndWhere(FieldMap{
		"`id`": {`IN`, 1, 3, 5},
	}))

	t.Log(sql, args)
}

func TestInsertObjs(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	sql, err := SqlInsertObjs(&args, &User{
		NickName: "one",
	})

	t.Log(sql, args)

	args = args[:0]
	sql, err = SqlInsertObjs(&args, []*User{
		{
			NickName: "1sdf",
		},
		{
			NickName: "2sdf",
			IsOn:     1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sql, args)
}

func TestUpdateByObj(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	sql, err := SqlUpdateByObj(&args, &User{
		Id:       12,
		NickName: "ban user",
		IsOn:     0,
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Log(sql, args)
}

func TestDeleteByObj(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	sql, err := SqlDeleteByObj(&args, &User{
		Id: 12,
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Log(sql, args)
}

func TestTransaction_Commit(t *testing.T) {
	tx, err := g.Begin()
	if err != nil {
		t.Fatal(err)
	}

	var (
		user        User
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	)

	defer func() {
		cancel()
	}()

	err = tx.FindOneObjContext(ctx, AndWhere(FieldMap{
		"updated_at": {0},
	}), &user)
	if err != nil {
		t.Fatal(err)
	}

	res, err := tx.UpdateObj(&user)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}

	if rows == 1 {
		_ = tx.Commit()
	} else {
		_ = tx.Rollback()
	}

	t.Log(user)
}

// BenchmarkMysqlQuery_Sql-4         951957              1088 ns/op            1000 B/op         20 allocs/op
func BenchmarkMysqlQuery_Sql(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			query := AcquireQuery4Mysql()
			args := base.AcquireArgs()

			query.From("`user`").Where(AndWhere(FieldMap{
				"`id`":     {`IN`, 1, 5, 10, 30, 23, 56},
				"`name`":   {`LIKE`, "dd%"},
				"`status`": {`>`, 0},
			}))

			query.Sql(&args)

			base.ReleaseArgs(&args)
			query.Close()
		}
	})
}

func BenchmarkMysqlQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		query := AcquireQuery4Mysql()
		args := base.AcquireArgs()

		query.From("`user`").Where(AndWhere(FieldMap{
			"`id`":     {`IN`, 1, 5, 10, 30, 23, 56},
			"`name`":   {`LIKE`, "dd%"},
			"`status`": {`>`, 0},
		}))

		query.Sql(&args)

		base.ReleaseArgs(&args)
		query.Close()
	}
}

func BenchmarkInsertObjs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		args := base.AcquireArgs()

		_, _ = SqlInsertObjs(&args, []*User{
			{
				NickName: "sdfasdf",
			},
			{
				NickName: "sdfadafasfsdf",
			},
			{
				NickName: "sdfasadfwsdf",
			},
			{
				NickName: "sd23432sdfsfdfasdf",
			},
		})

		base.ReleaseArgs(&args)
	}
}

func BenchmarkDeleteByObj(b *testing.B) {
	for i := 0; i < b.N; i++ {
		args := base.AcquireArgs()
		_, _ = SqlDeleteByObj(&args, &User{
			Id: 14,
		})

		base.ReleaseArgs(&args)
	}
}

func BenchmarkGroup_Insert(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := g.Insert(`user`, Row{
				"nickname": strconv.FormatInt(time.Now().UnixNano(), 10),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGroup_InsertObj(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			user := User{
				NickName: strconv.FormatInt(time.Now().UnixNano(), 10),
			}
			_, err := g.InsertObj(&user)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
