package orm

import (
	"context"
	"database/sql"
	"time"
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
	Query(sqlStr string, args ...interface{}) (rows *sql.Rows, err error)
	Execute(sqlStr string, args ...interface{}) (result sql.Result, err error)
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type mysqlPool struct {
	Pool

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

func (mp *mysqlPool) Execute(sqlStr string, args ...interface{}) (result sql.Result, err error) {
	return mp.db.Exec(sqlStr, args...)
}

func (mp *mysqlPool) Begin() (*sql.Tx, error) {
	return mp.db.Begin()
}

func (mp *mysqlPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return mp.db.BeginTx(ctx, opts)
}
