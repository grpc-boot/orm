package orm

import "strings"

type WhereCondition struct {
	opt       string
	condition Condition
}

type Where []WhereCondition

func AndWhere(condition Condition) WhereCondition {
	return WhereCondition{
		opt:       And,
		condition: condition,
	}
}

func OrWhere(condition Condition) WhereCondition {
	return WhereCondition{
		opt:       Or,
		condition: condition,
	}
}

func (w Where) Sql(args *[]interface{}) (sql string) {
	var (
		buf strings.Builder
	)

	for index, wc := range w {
		sqlStr := wc.condition.Sql(args)
		if sqlStr == "" {
			continue
		}

		if index == 0 {
			buf.WriteString(` WHERE `)
		} else {
			buf.WriteByte(' ')
			buf.WriteString(wc.opt)
			buf.WriteByte(' ')
		}
		buf.WriteString(sqlStr)
	}

	return buf.String()
}
