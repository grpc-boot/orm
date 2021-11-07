package orm

import (
	"strings"
)

const (
	Or  = `OR`
	And = `AND`
)

type Condition interface {
	Opt() (opt string)
	Sql(args *[]interface{}) (sql string)
}

func OrCondition(fields map[string][]interface{}) Condition {
	return condition{
		opt:    Or,
		fields: fields,
	}
}

func AndCondition(fields map[string][]interface{}) Condition {
	return condition{
		opt:    And,
		fields: fields,
	}
}

type condition struct {
	Condition

	opt    string
	fields map[string][]interface{}
}

func (c condition) Opt() (opt string) {
	return c.opt
}

func (c condition) Sql(args *[]interface{}) (sql string) {
	if len(c.fields) < 1 {
		return
	}

	var (
		buf          strings.Builder
		operator     string
		hasCondition bool
	)

	for field, value := range c.fields {
		if len(value) < 1 {
			continue
		}

		if len(value) > 1 {
			operator = strings.ToUpper(value[0].(string))
			*args = append(*args, value[1:]...)
		} else {
			operator = `=`
			*args = append(*args, value...)
		}

		if !hasCondition {
			hasCondition = true
			buf.WriteByte('(')
		} else {
			buf.WriteByte(' ')
			buf.WriteString(c.opt)
			buf.WriteByte(' ')
		}

		buf.WriteString(field)

		switch operator {
		case "IN":
			buf.WriteString(" IN(")
			for index := 1; index < len(value); index++ {
				if index > 1 {
					buf.WriteByte(',')
				}
				buf.WriteByte('?')
			}
			buf.WriteByte(')')
		case "BETWEEN":
			buf.WriteString(" BETWEEN ? AND ?")
		default:
			buf.WriteByte(' ')
			buf.WriteString(operator)
			buf.WriteString(" ?")
		}
	}

	if !hasCondition {
		return
	}

	buf.WriteByte(')')
	return buf.String()
}
