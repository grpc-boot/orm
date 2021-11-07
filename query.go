package orm

import (
	"database/sql"
	"strconv"
	"strings"
	"sync"
)

var (
	defaultColumns = "*"

	mysqlQueryPool = &sync.Pool{
		New: func() interface{} {
			query := &mysqlQuery{
				columns: defaultColumns,
			}
			return query
		},
	}
)

type Query interface {
	Db(g Group) Query
	Select(columns ...string) Query
	From(table string) Query
	Where(condition Condition) Query
	AndWhere(condition Condition) Query
	OrWhere(condition Condition) Query
	Group(fields ...string) Query
	Having(having string) Query
	Order(orders ...string) Query
	Offset(offset int64) Query
	Limit(limit int64) Query
	Sql(arguments *[]interface{}) (sql string)
	One(useMaster bool) (rows *sql.Rows, err error)
	All(useMaster bool) (rows *sql.Rows, err error)
	Close()
}

func NewMysqlQuery() Query {
	return mysqlQueryPool.Get().(Query)
}

type mysqlQuery struct {
	Query

	g       Group
	table   string
	columns string
	where   Where
	group   string
	having  string
	order   string
	offset  int64
	limit   int64
}

func (mq *mysqlQuery) reset() Query {
	mq.g = nil
	mq.table = ""
	mq.columns = defaultColumns
	mq.offset = 0
	mq.limit = 0
	mq.group = ""
	mq.having = ""
	mq.order = ""

	if len(mq.where) > 0 {
		mq.where = mq.where[:0]
	}
	return mq
}

func (mq *mysqlQuery) Db(g Group) Query {
	mq.g = g
	return mq
}

func (mq *mysqlQuery) Select(columns ...string) Query {
	mq.columns = strings.Join(columns, ",")
	return mq
}

func (mq *mysqlQuery) From(table string) Query {
	mq.table = table
	return mq
}

func (mq *mysqlQuery) Where(condition Condition) Query {
	if len(mq.where) == 0 {
		mq.where = Where{AndWhere(condition)}
	} else {
		mq.where = append(mq.where, AndWhere(condition))
	}
	return mq
}

func (mq *mysqlQuery) AndWhere(condition Condition) Query {
	return mq.Where(condition)
}

func (mq *mysqlQuery) OrWhere(condition Condition) Query {
	if len(mq.where) == 0 {
		mq.where = Where{OrWhere(condition)}
	} else {
		mq.where = append(mq.where, OrWhere(condition))
	}
	return mq
}

func (mq *mysqlQuery) Group(fields ...string) Query {
	mq.group = " GROUP BY " + strings.Join(fields, ",")
	return mq
}

func (mq *mysqlQuery) Having(having string) Query {
	mq.having = " HAVING " + having
	return mq
}

func (mq *mysqlQuery) Order(orders ...string) Query {
	mq.order = " ORDER BY " + strings.Join(orders, ",")
	return mq
}

func (mq *mysqlQuery) Offset(offset int64) Query {
	mq.offset = offset
	return mq
}

func (mq *mysqlQuery) Limit(limit int64) Query {
	mq.limit = limit
	return mq
}

func (mq *mysqlQuery) Close() {
	mq.reset()
	mysqlQueryPool.Put(mq)
}

func (mq *mysqlQuery) Sql(arguments *[]interface{}) (sql string) {
	var (
		whereStr  string
		sqlBuffer strings.Builder
	)

	sqlBuffer.WriteString(`SELECT `)
	sqlBuffer.WriteString(mq.columns)
	sqlBuffer.WriteString(` FROM `)
	sqlBuffer.WriteString(mq.table)

	if len(mq.where) > 0 {
		whereStr = mq.where.Sql(arguments)
		sqlBuffer.WriteString(whereStr)
	}
	sqlBuffer.WriteString(mq.group)
	sqlBuffer.WriteString(mq.having)
	sqlBuffer.WriteString(mq.order)

	if mq.limit < 1 {
		return sqlBuffer.String()
	}

	sqlBuffer.WriteString(" LIMIT ")
	sqlBuffer.WriteString(strconv.FormatInt(mq.offset, 10))
	sqlBuffer.WriteString(",")
	sqlBuffer.WriteString(strconv.FormatInt(mq.limit, 10))
	return sqlBuffer.String()
}

func (mq *mysqlQuery) One(useMaster bool) (rows *sql.Rows, err error) {
	mq.Limit(1)
	return mq.g.Query(mq, useMaster)
}

func (mq *mysqlQuery) All(useMaster bool) (rows *sql.Rows, err error) {
	return mq.g.Query(mq, useMaster)
}
