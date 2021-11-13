package orm

// go test . -v
// go test -bench=. -benchmem -benchtime=5s
// brew services start mysql
import (
	"context"
	"log"
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
		},
		Slaves: []PoolOption{
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
	cond := map[string][]interface{}{
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
	con := OrCondition(map[string][]interface{}{
		"`id`":    {13},
		"`name`":  {`LIKE`, "ma%"},
		"`age`":   {`BETWEEN`, 10, 20},
		"`month`": {`IN`, 1, 6, 7},
	})

	con1 := OrCondition(map[string][]interface{}{
		"`id`": {`>=`, 13},
	})

	w := Where{
		AndWhere(con1), OrWhere(con),
	}

	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)
	t.Log(w.Sql(&args), args)
}

func TestMysqlQuery_Sql(t *testing.T) {
	query := NewMysqlQuery()
	defer query.Close()

	query.From("`user`").Select("`id`", "`name`")
	query.Where(OrCondition(map[string][]interface{}{
		"`id`":    {`IN`, 12, 45, 67},
		"`is_on`": {1},
	}))

	query.AndWhere(AndCondition(map[string][]interface{}{
		"`id`": {`>`, 10},
	}))

	query.OrWhere(AndCondition(map[string][]interface{}{
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

	sql := SqlInsert(&args, "`user`", map[string]interface{}{
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

	sql := SqlUpdate(&args, "`user`", map[string]interface{}{
		"`name`":       time.Now().String(),
		"`created_at`": time.Now().UnixNano(),
	}, AndCondition(map[string][]interface{}{
		"`id`": {`IN`, 1, 3, 5},
	}))

	t.Log(sql, args)
}

func TestDeleteAll(t *testing.T) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sql := SqlDelete(&args, "`user`", AndCondition(map[string][]interface{}{
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

	err = tx.FindOneObjContext(ctx, AndCondition(map[string][]interface{}{
		"id":         {1},
		"updated_at": {0},
	}), &user)
	if err != nil {
		t.Fatal(err)
	}

	if user.Id < 1 {
		t.Fatalf("want %d, got 0", user.Id)
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
			query := NewMysqlQuery()
			args := base.AcquireArgs()

			query.From("`user`").Where(AndCondition(map[string][]interface{}{
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
		query := NewMysqlQuery()
		args := base.AcquireArgs()

		query.From("`user`").Where(AndCondition(map[string][]interface{}{
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
