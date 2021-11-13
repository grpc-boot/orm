package orm

import (
	"context"
	"database/sql"

	"github.com/grpc-boot/base"
)

type Transaction interface {
	Commit() (err error)
	Rollback() (err error)
	Query(sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	QueryContext(ctx context.Context, sqlStr string, args ...interface{}) (rows []map[string]string, err error)
	Exec(sqlStr string, args ...interface{}) (result sql.Result, err error)
	ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error)
	InsertObj(obj interface{}) (result sql.Result, err error)
	InsertObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	DeleteObj(obj interface{}) (result sql.Result, err error)
	DeleteObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	UpdateObj(obj interface{}) (result sql.Result, err error)
	UpdateObjContext(ctx context.Context, obj interface{}) (result sql.Result, err error)
	Find(query Query) (rows []map[string]string, err error)
	FindContext(ctx context.Context, query Query) (rows []map[string]string, err error)
	FindAll(query Query, obj interface{}) (objList interface{}, err error)
	FindAllContext(ctx context.Context, query Query, obj interface{}) (objList interface{}, err error)
	FindOne(table string, condition Condition) (row map[string]string, err error)
	FindOneContext(ctx context.Context, table string, condition Condition) (row map[string]string, err error)
	FindOneObj(condition Condition, obj interface{}) (err error)
	FindOneObjContext(ctx context.Context, condition Condition, obj interface{}) (err error)
	Insert(table string, rows ...map[string]interface{}) (result sql.Result, err error)
	InsertContext(ctx context.Context, table string, rows ...map[string]interface{}) (result sql.Result, err error)
	DeleteAll(table string, condition Condition) (result sql.Result, err error)
	DeleteAllContext(ctx context.Context, table string, condition Condition) (result sql.Result, err error)
	UpdateAll(table string, set map[string]interface{}, condition Condition) (result sql.Result, err error)
	UpdateAllContext(ctx context.Context, table string, set map[string]interface{}, condition Condition) (result sql.Result, err error)
}

type transaction struct {
	Transaction

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

func (t *transaction) FindAll(query Query, obj interface{}) (objList interface{}, err error) {
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

func (t *transaction) FindAllContext(ctx context.Context, query Query, obj interface{}) (objList interface{}, err error) {
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

func (t *transaction) FindOne(table string, condition Condition) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = NewMysqlQuery().From(table).Where(condition).Limit(1)
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

func (t *transaction) FindOneContext(ctx context.Context, table string, condition Condition) (row map[string]string, err error) {
	var (
		rows *sql.Rows

		args   = base.AcquireArgs()
		query  = NewMysqlQuery().From(table).Where(condition).Limit(1)
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

func (t *transaction) FindOneObj(condition Condition, obj interface{}) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, condition, obj)
	if err != nil {
		return err
	}

	rows, err = t.tx.Query(sqlStr, args...)
	if err != nil {
		return
	}

	return ToObj(rows, obj)
}

func (t *transaction) FindOneObjContext(ctx context.Context, condition Condition, obj interface{}) (err error) {
	var (
		args = base.AcquireArgs()
		rows *sql.Rows
	)

	defer base.ReleaseArgs(&args)

	sqlStr, err := SqlFindOneObj(&args, condition, obj)
	if err != nil {
		return err
	}

	rows, err = t.tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return
	}

	return ToObj(rows, obj)
}

func (t *transaction) Insert(table string, rows ...map[string]interface{}) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) InsertContext(ctx context.Context, table string, rows ...map[string]interface{}) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlInsert(&args, table, rows...)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) DeleteAll(table string, condition Condition) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, condition)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) DeleteAllContext(ctx context.Context, table string, condition Condition) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlDelete(&args, table, condition)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.ExecContext(ctx, sqlStr, args...)
}

func (t *transaction) UpdateAll(table string, set map[string]interface{}, condition Condition) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, condition)
	)
	defer base.ReleaseArgs(&args)

	return t.tx.Exec(sqlStr, args...)
}

func (t *transaction) UpdateAllContext(ctx context.Context, table string, set map[string]interface{}, condition Condition) (result sql.Result, err error) {
	var (
		args   = base.AcquireArgs()
		sqlStr = SqlUpdate(&args, table, set, condition)
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
