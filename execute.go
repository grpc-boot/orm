package orm

import (
	"strings"
)

// Row 行
type Row map[string]interface{}

// SqlInsert 生成插入sql
func SqlInsert(args *[]interface{}, table string, rows ...Row) (sql string) {
	if len(rows) < 0 {
		return ""
	}

	var (
		sqlBuffer strings.Builder

		first       = true
		row         = rows[0]
		dbFieldList = make([]string, 0, len(rows))
		v           = make([]byte, 0, 2*len(rows))
	)

	for field, value := range row {
		if first {
			first = false
			v = append(v, '(')
			sqlBuffer.WriteString("INSERT INTO ")
			sqlBuffer.WriteString(table)
			sqlBuffer.WriteByte('(')
		} else {
			v = append(v, ',')
			sqlBuffer.WriteByte(',')
		}

		dbFieldList = append(dbFieldList, field)
		v = append(v, '?')
		sqlBuffer.WriteString(field)
		*args = append(*args, value)
	}

	//没有找到字段
	if first {
		return ""
	}

	sqlBuffer.WriteByte(')')
	v = append(v, ')')

	sqlBuffer.WriteString("VALUES")
	sqlBuffer.Write(v)

	if len(rows) > 1 {
		for start := 1; start < len(rows); start++ {
			sqlBuffer.WriteByte(',')
			sqlBuffer.Write(v)
			for _, field := range dbFieldList {
				*args = append(*args, rows[start][field])
			}
		}
	}

	return sqlBuffer.String()
}

// SqlUpdate 生成更新sql
func SqlUpdate(args *[]interface{}, table string, set Row, where Where) (sql string) {
	var (
		sqlBuffer strings.Builder

		num = 0
	)
	sqlBuffer.WriteString(`UPDATE `)
	sqlBuffer.WriteString(table)
	sqlBuffer.WriteString(` SET `)

	for field, arg := range set {
		if num > 0 {
			sqlBuffer.WriteByte(',')
		} else {
			num++
		}

		sqlBuffer.WriteString(field)
		sqlBuffer.WriteString("=?")
		*args = append(*args, arg)
	}

	if where != nil {
		sqlBuffer.WriteString(where.Sql(args))
	}

	return sqlBuffer.String()
}

// SqlDelete 生成删除sql
func SqlDelete(args *[]interface{}, table string, where Where) (sql string) {
	var (
		sqlBuffer strings.Builder
	)
	sqlBuffer.WriteString("DELETE FROM ")
	sqlBuffer.WriteString(table)

	if where != nil {
		sqlBuffer.WriteString(where.Sql(args))
	}
	return sqlBuffer.String()
}
