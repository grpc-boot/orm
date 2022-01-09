package orm

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/grpc-boot/base"
)

func (g *group) Tables(pattern string, useMaster bool) (tableList []string, err error) {
	var (
		sqlRows *sql.Rows
		sqlStr  = `SHOW TABLES`
	)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		if pattern == "" {
			return mPool.Query(sqlStr)
		}
		return mPool.Query(sqlStr + ` LIKE '` + pattern + `'`)
	}, useMaster)

	if err != nil {
		return
	}

	tableList = []string{}

	FormatRows(sqlRows, func(fieldValue map[string][]byte) {
		for _, v := range fieldValue {
			tableList = append(tableList, base.Bytes2String(v))
			return
		}
	})

	return tableList, nil
}

func (g *group) Table(table string, useMaster bool) (t *Table, err error) {
	var (
		sqlRows *sql.Rows
		sqlStr  = `SHOW FULL COLUMNS FROM ` + table
	)

	sqlRows, err = g.query(func(mPool Pool) (*sql.Rows, error) {
		return mPool.Query(sqlStr)
	}, useMaster)

	if err != nil {
		return
	}

	t = &Table{Name: table, Columns: []Column{}}

	FormatRows(sqlRows, func(fieldValue map[string][]byte) {
		column := Column{
			Field:      base.Bytes2String(fieldValue["Field"]),
			Type:       base.Bytes2String(fieldValue["Type"]),
			Collation:  base.Bytes2String(fieldValue["Collation"]),
			Null:       "YES" == base.Bytes2String(fieldValue["Null"]),
			Key:        base.Bytes2String(fieldValue["Key"]),
			Default:    base.Bytes2String(fieldValue["Default"]),
			Extra:      base.Bytes2String(fieldValue["Extra"]),
			Privileges: base.Bytes2String(fieldValue["Privileges"]),
			Comment:    base.Bytes2String(fieldValue["Comment"]),
		}

		column.Unsigned = strings.Contains(column.Type, "unsigned")
		column.Type = strings.Split(column.Type, " ")[0]

		index := strings.IndexByte(column.Type, '(')
		if index > 0 {
			lp := column.Type[index+1 : len(column.Type)-1]
			pIndex := strings.IndexByte(lp, ',')
			if pIndex > 0 {
				column.Length, _ = strconv.Atoi(lp[:pIndex])
				column.Point, _ = strconv.Atoi(lp[pIndex+1:])
			} else {
				column.Length, _ = strconv.Atoi(lp)
			}

			column.Type = column.Type[:index]
		}

		t.Columns = append(t.Columns, column)
	})

	return t, nil
}
