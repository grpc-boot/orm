package orm

import (
	"context"
	"database/sql"

	"github.com/grpc-boot/base"
)

type Transaction interface {
	// Commit 提交事务
	Commit() (err error)
	// Rollback 回滚事务
	Rollback() (err error)
	// Query 查询
	Query(sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	// QueryContext with context 查询
	QueryContext(ctx context.Context, sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	// Exec 执行
	Exec(sqlStr string, args ...interface{}) (result sql.Result, err error)
	// ExecContext with context 执行
	ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error)
	// InsertObj 插入对象
	InsertObj(obj interface{}) (result sql.Result, err error)
	// InsertObjContext with context 插入对象
	InsertObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// DeleteObj 删除对象
	DeleteObj(obj interface{}) (result sql.Result, err error)
	// DeleteObjContext with context 删除对象
	DeleteObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// UpdateObj 更新对象
	UpdateObj(obj interface{}) (result sql.Result, err error)
	// UpdateObjContext with context 更新对象
	UpdateObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	// Find 根据Query查询
	Find(query Query) (rows []map[string]string, err error)
	// FindContext with context 根据Query查询
	FindContext(ctx context.Context, query Query) (rows []map[string]string, err error)
	// FindAll 根据Query查询，返回对象列表
	FindAll(query Query, obj interface{}) (objList []interface{}, err error)
	// FindAllContext with context 根据Query查询，返回对象列表
	FindAllContext(ctx context.Context, query Query, obj interface{}) (objList []interface{}, err error)
	// FindOne 查询一个
	FindOne(table string, where Where) (row map[string]string, err error)
	// FindOneContext with context  查询一个
	FindOneContext(ctx context.Context, table string, where Where) (row map[string]string, err error)
	// FindOneObj 查询一个对象
	FindOneObj(where Where, obj interface{}) (err error)
	// FindOneObjContext with context  查询一个对象
	FindOneObjContext(ctx context.Context, where Where, obj interface{}) (err error)
	// Insert 插入
	Insert(table string, rows ...Row) (result sql.Result, err error)
	// InsertContext with context 插入
	InsertContext(ctx context.Context, table string, rows ...Row) (result sql.Result, err error)
	// DeleteAll 删除
	DeleteAll(table string, where Where) (result sql.Result, err error)
	// DeleteAllContext with context 删除
	DeleteAllContext(ctx context.Context, table string, where Where) (result sql.Result, err error)
	// UpdateAll 更新
	UpdateAll(table string, set Row, where Where) (result sql.Result, err error)
	// UpdateAllContext with context  更新
	UpdateAllContext(ctx context.Context, table string, set Row, where Where) (result sql.Result, err error)
}

type transaction struct {
	tx *sql.Tx
}

func newTx(tx *sql.Tx) Transaction {
	return &transaction{tx: tx}
}

func (t *transaction) Query(sqlStr string, args ...interface{}) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows
	)

	sqlRows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (t *transaction) QueryContext(ctx context.Context, sqlStr string, args ...interface{}) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows
	)

	sqlRows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (t *transaction) Exec(sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) InsertObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlInsertObjs(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) InsertObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlInsertObjs(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) DeleteObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlDeleteByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) DeleteObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlDeleteByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) UpdateObj(obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlUpdateByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) UpdateObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error) {
	args := base.AcquireArgs()
	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlUpdateByObj(&args, obj)
	if err != nil {
		return nil, err
	}

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) Find(query Query) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (t *transaction) FindContext(ctx context.Context, query Query) (rows []map[string]string, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	return ToMap(sqlRows)
}

func (t *transaction) FindAll(query Query, obj interface{}) (objList []interface{}, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	return ToObjList(sqlRows, obj)
}

func (t *transaction) FindAllContext(ctx context.Context, query Query, obj interface{}) (objList []interface{}, err error) {
	var (
		sqlRows *sql.Rows

		args   = base.AcquireArgs()
		sqlStr = query.Sql(&args)
	)

	defer base.ReleaseArgs(&args)

	sqlRows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	return ToObjList(sqlRows, obj)
}

func (t *transaction) FindOne(table string, where Where) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = AcquireQuery4Mysql().From(table).Where(where).Limit(1)
		sqlStr = query.Sql(&args)
	)

	defer func() {
		base.ReleaseArgs(&args)
		query.Close()
	}()

	rows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	tRows, err := ToMap(rows)
	if err != nil {
		return
	}

	if len(tRows) > 0 {
		return tRows[0], nil
	}
	return
}

func (t *transaction) FindOneContext(ctx context.Context, table string, where Where) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = AcquireQuery4Mysql().From(table).Where(where).Limit(1)
		sqlStr = query.Sql(&args)
	)

	defer func() {
		base.ReleaseArgs(&args)
		query.Close()
	}()

	rows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	tRows, err := ToMap(rows)
	if err != nil {
		return
	}

	if len(tRows) > 0 {
		return tRows[0], nil
	}
	return
}

func (t *transaction) FindOneObj(where Where, obj interface{}) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, where, obj)
	if err != nil {
		return err
	}

	rows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	return ToObj(rows, obj)
}

func (t *transaction) FindOneObjContext(ctx context.Context, where Where, obj interface{}) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, where, obj)
	if err != nil {
		return err
	}

	rows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	return ToObj(rows, obj)
}

func (t *transaction) Insert(table string, rows ...Row) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) InsertContext(ctx context.Context, table string, rows ...Row) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) DeleteAll(table string, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, where)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) DeleteAllContext(ctx context.Context, table string, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, where)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) UpdateAll(table string, set Row, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, where)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) UpdateAllContext(ctx context.Context, table string, set Row, where Where) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, where)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) Commit() (err error) {
	return t.tx.Commit()
}

func (t *transaction) Rollback() (err error) {
	return t.tx.Rollback()
}
