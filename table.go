package orm

import (
	"fmt"
	"strings"

	"github.com/grpc-boot/base"
)

const (
	Bit        = "bit"
	TinyInt    = "tinyint"
	SmallInt   = "smallint"
	MediumInt  = "mediumint"
	Int        = "int"
	BigInt     = "bigint"
	Float      = "float"
	Double     = "double"
	Decimal    = "decimal"
	TinyText   = "tinytext"
	MediumText = "mediumtext"
	Text       = "text"
	LongText   = "longtext"
	Json       = "json"
	Varchar    = "varchar"
	Char       = "char"
	Timestamp  = "timestamp"
	Date       = "date"
	Datetime   = "datetime"
	TinyBlob   = "tinyblob"
	MediumBlob = "mediumblob"
	Blob       = "blob"
	LongBlob   = "longblob"
	Set        = "set"
	Enum       = "enum"
)

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Column struct {
	Field      string `json:"field"`
	Type       string `json:"type"`
	Length     int    `json:"length"`
	Point      int    `json:"point"`
	Unsigned   bool   `json:"unsigned"`
	Collation  string `json:"collation"`
	Null       bool   `json:"null"`
	Key        string `json:"key"`
	Default    string `json:"default"`
	Extra      string `json:"extra"`
	Privileges string `json:"privileges"`
	Comment    string `json:"comment"`
}

// GoType 转换为go类型
func (c *Column) GoType() string {
	switch c.Type {
	case Bit, TinyInt:
		if c.Unsigned {
			return "uint8"
		}
		return "int8"
	case SmallInt, MediumInt, Int, BigInt:
		if c.Unsigned {
			return "uint64"
		}
		return "int64"
	case Float, Double, Decimal:
		return "float64"
	case TinyBlob, MediumBlob, Blob, LongBlob:
		return "[]byte"
	}
	return "string"
}

func (c *Column) ToProperty() string {
	return fmt.Sprintf("%s %s `json:\"%s\" borm:\"%s\"`", base.BigCamels('_', c.Field), c.GoType(), c.Field, c.Field)
}

// ToStruct 转换为go结构体字符串
func (t *Table) ToStruct() string {
	var (
		buf strings.Builder
	)

	buf.WriteString("type ")
	buf.WriteString(base.BigCamels('_', t.Name))
	buf.WriteString(" struct {\n")
	for _, c := range t.Columns {
		buf.WriteString("    ")
		buf.WriteString(c.ToProperty())
		buf.WriteString("\n")
	}

	buf.WriteString("}")
	return buf.String()
}
