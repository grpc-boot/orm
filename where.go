package orm

import "strings"

type Where []Condition

func (w *Where) Sql(args *[]interface{}) (sql string) {
	var (
		buf strings.Builder
	)

	for index, where := range *w {
		sqlStr := where.Sql(args)
		if sqlStr == "" {
			continue
		}

		if index == 0 {
			buf.WriteString(` WHERE `)
		} else {
			buf.WriteByte(' ')
			buf.WriteString(where.Opt())
			buf.WriteByte(' ')
		}
		buf.WriteString(sqlStr)
	}

	return buf.String()
}
