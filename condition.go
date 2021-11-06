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

func orCondition(fields map[string][]interface{}) Condition {
	return &condition{
		opt:    Or,
		fields: fields,
	}
}

func andCondition(fields map[string][]interface{}) Condition {
	return &condition{
		opt:    And,
		fields: fields,
	}
}

type condition struct {
	Condition

	opt    string
	fields map[string][]interface{}
}

func (c *condition) Opt() (opt string) {
	return c.opt
}

func (c *condition) Sql(args *[]interface{}) (sql string) {
	if len(c.fields) < 1 {
		return
	}

	var (
		buf          strings.Builder
		operator     string
		position     int
		hasCondition bool
	)

	for field, value := range c.fields {
		if len(value) < 1 {
			continue
		}

		operator = "="
		position = strings.Index(field, " ")
		if position > 0 {
			operator = strings.ToUpper(field[position+1:])
			field = field[:position]
		}

		if len(value) > 1 && operator != "BETWEEN" {
			operator = "IN"
		}

		if !hasCondition {
			hasCondition = true
			buf.WriteString("(")
		} else {
			buf.WriteString(" AND ")
		}
		buf.WriteString(field)

		switch operator {
		case "IN":
			buf.WriteString(" IN(")
			buf.WriteString(strings.Repeat(inHolder, len(value))[:2*len(value)-1])
			buf.WriteString(")")
		case "BETWEEN":
			buf.WriteString(" BETWEEN ? AND ?")
		case "LIKE":
			buf.WriteString(" LIKE ?")
		default:
			buf.WriteString(" ")
			buf.WriteString(operator)
			buf.WriteString(" ?")
		}
		*args = append(*args, value...)
	}

	if !hasCondition {
		return
	}

	buf.WriteString(`)`)
	return buf.String()
}
