package orm

import (
	"strconv"
	"strings"
	"sync"
)

const (
	inHolder = "?,"
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
	Select(columns ...string) Query
	From(table string) Query
	Where(where map[string][]interface{}) Query
	AndWhere(where map[string][]interface{}) Query
	OrWhere(where map[string][]interface{}) Query
	Group(fields ...string) Query
	Having(having string) Query
	Order(orders ...string) Query
	Offset(offset int64) Query
	Limit(limit int64) Query
	Sql(arguments *[]interface{}) (sql string)
	Close()
}

func NewMysqlQuery() Query {
	return mysqlQueryPool.Get().(Query)
}

type mysqlQuery struct {
	Query

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

func (mq *mysqlQuery) Select(columns ...string) Query {
	mq.columns = strings.Join(columns, ",")
	return mq
}

func (mq *mysqlQuery) From(table string) Query {
	mq.table = table
	return mq
}

func (mq *mysqlQuery) Where(where map[string][]interface{}) Query {
	if len(mq.where) == 0 {
		mq.where = []Condition{andCondition(where)}
	} else {
		mq.where = append(mq.where, andCondition(where))
	}
	return mq
}

func (mq *mysqlQuery) AndWhere(where map[string][]interface{}) Query {
	return mq.Where(where)
}

func (mq *mysqlQuery) OrWhere(where map[string][]interface{}) Query {
	if len(mq.where) == 0 {
		mq.where = []Condition{orCondition(where)}
	} else {
		mq.where = append(mq.where, orCondition(where))
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
		where     string
		sqlBuffer strings.Builder
	)

	sqlBuffer.WriteString(`SELECT `)
	sqlBuffer.WriteString(mq.columns)
	sqlBuffer.WriteString(` FROM `)
	sqlBuffer.WriteString(mq.table)

	if len(mq.where) > 0 {
		where = mq.where.Sql(arguments)
		sqlBuffer.WriteString(where)
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
