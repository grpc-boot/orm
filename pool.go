package orm

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type PoolOption struct {
	//格式："userName:password@schema(host:port)/dbName"，如：root:123456@tcp(127.0.0.1:3306)/test
	Dsn string `yaml:"dsn" json:"dsn"`
	//单位s
	MaxConnLifetime int `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxOpenConns    int `yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns    int `yaml:"maxIdleConns" json:"maxIdleConns"`
}

type Pool interface {
	// Query 查询
	Query(sqlStr string, args ...interface{}) (rows *sql.Rows, err error)
	// QueryContext with context 查询
	QueryContext(ctx context.Context, sqlStr string, args ...interface{}) (rows *sql.Rows, err error)
	// Exec 执行
	Exec(sqlStr string, args ...interface{}) (result sql.Result, err error)
	// ExecContext with context 执行
	ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error)
	// Begin 开启事务
	Begin() (Transaction, error)
	// BeginTx with context 开启事务
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
}

type mysqlPool struct {
	db *sql.DB
}

func newMysqlPool(option *PoolOption) (Pool, error) {
	db, err := sql.Open("mysql", option.Dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Duration(option.MaxConnLifetime) * time.Second)
	db.SetMaxIdleConns(option.MaxIdleConns)
	db.SetMaxOpenConns(option.MaxOpenConns)

	return &mysqlPool{
		db: db,
	}, nil
}

func (mp *mysqlPool) Query(sqlStr string, args ...interface{}) (rows *sql.Rows, err error) {
	return mp.db.Query(sqlStr, args...)
}

func (mp *mysqlPool) QueryContext(ctx context.Context, sqlStr string, args ...interface{}) (rows *sql.Rows, err error) {
	return mp.db.QueryContext(ctx, sqlStr, args...)
}

func (mp *mysqlPool) Exec(sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return mp.db.Exec(sqlStr, args...)
}

func (mp *mysqlPool) ExecContext(ctx context.Context, sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return mp.db.ExecContext(ctx, sqlStr, args...)
}

func (mp *mysqlPool) Begin() (Transaction, error) {
	tx, err := mp.db.Begin()
	if err != nil {
		return nil, err
	}

	return newTx(tx), err
}

func (mp *mysqlPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	tx, err := mp.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return newTx(tx), err
}
