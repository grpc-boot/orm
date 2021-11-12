package orm

import (
	"errors"
	"reflect"
	"strings"
)

const (
	tagName  = `borm`
	required = `required`
	primary  = `primary`
)

const (
	tableMethod  = `tableName`
	beforeSave   = `BeforeSave`
	beforeUpdate = `BeforeUpdate`
	beforeCreate = `BeforeCreate`
)

var (
	ErrInvalidRowsTypes     = errors.New(`only *struct or []*struct types are supported`)
	ErrNotFoundField        = errors.New(`failed to match the field from the struct to the database. Please configure the borm tag correctly`)
	ErrNotFoundPrimaryField = errors.New(`failed to found primary field. Please configure the primary on borm tag correctly`)
	ErrInvalidTypes         = errors.New(`only *struct types are supported`)
	ErrInvalidFieldTypes    = errors.New(`only bool(1 is true, other is false),string、float64、float32、int、uint、int8、uint8、int16、uint16、int32、uint32、int64 and uint64 types are supported`)
)

func tableName(value reflect.Value) (tableName string) {
	v := value.MethodByName(tableMethod)
	if v.Kind() == reflect.Func {
		return v.Call(nil)[0].String()
	}
	return strings.ToLower(value.Type().Name())
}

func SqlFindOneObj(args *[]interface{}, condition Condition, obj interface{}) (sql string, err error) {
	var (
		value = reflect.ValueOf(obj)
	)

	if value.Kind() != reflect.Ptr {
		return "", ErrInvalidTypes
	}

	value = value.Elem()

	if value.Kind() != reflect.Struct {
		return "", ErrInvalidTypes
	}

	var (
		sqlBuffer strings.Builder
	)

	sqlBuffer.WriteString("SELECT * FROM `")
	sqlBuffer.WriteString(tableName(value))
	sqlBuffer.WriteByte('`')
	sqlBuffer.WriteString(Where{AndWhere(condition)}.Sql(args))
	sqlBuffer.WriteString(` LIMIT 1`)
	return sqlBuffer.String(), nil
}

func SqlInsertObjs(args *[]interface{}, rows interface{}) (sql string, err error) {
	var (
		vRows  = reflect.ValueOf(rows)
		values []reflect.Value
	)

	switch vRows.Kind() {
	case reflect.Slice:
		values = make([]reflect.Value, 0, vRows.Len())
		for i := 0; i < vRows.Len(); i++ {
			value := vRows.Index(i)

			if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
				return "", ErrInvalidRowsTypes
			}

			if bs := value.MethodByName(beforeSave); bs.Kind() == reflect.Func {
				bs.Call(nil)
			}

			if bc := value.MethodByName(beforeCreate); bc.Kind() == reflect.Func {
				bc.Call(nil)
			}
			values = append(values, value.Elem())
		}
	case reflect.Ptr:
		if vRows.Elem().Kind() != reflect.Struct {
			return "", ErrInvalidRowsTypes
		}

		if bs := vRows.MethodByName(beforeSave); bs.Kind() == reflect.Func {
			bs.Call(nil)
		}

		if bc := vRows.MethodByName(beforeCreate); bc.Kind() == reflect.Func {
			bc.Call(nil)
		}
		values = []reflect.Value{vRows.Elem()}
	default:
		return "", ErrInvalidRowsTypes
	}

	var (
		sqlBuffer    strings.Builder
		dbFieldCount int
		dbField      string
		isRequired   bool

		value       = values[0]
		t           = value.Type()
		fieldCount  = t.NumField()
		dbFieldList = make([]string, 0, len(values))
		v           = make([]byte, 0, 2*fieldCount)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isRequired = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = tags[0]
		for _, val := range tags {
			if val == required {
				isRequired = true
				break
			}
		}

		if !isRequired && value.Field(i).IsZero() {
			continue
		}

		dbFieldCount++
		dbFieldList = append(dbFieldList, t.Field(i).Name)

		if dbFieldCount > 1 {
			v = append(v, ',')
			sqlBuffer.WriteByte(',')
		} else {
			v = append(v, '(')
			sqlBuffer.WriteString("INSERT INTO ")
			sqlBuffer.WriteByte('`')
			sqlBuffer.WriteString(tableName(value))
			sqlBuffer.WriteByte('`')
			sqlBuffer.WriteByte('(')
		}

		v = append(v, '?')
		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString(dbField)
		sqlBuffer.WriteByte('`')
		*args = append(*args, value.Field(i).Interface())
	}

	//没有找到字段
	if dbFieldCount < 1 {
		return "", ErrNotFoundField
	}

	sqlBuffer.WriteByte(')')
	v = append(v, ')')

	sqlBuffer.WriteString("VALUES")
	sqlBuffer.Write(v)

	if len(values) > 1 {
		for start := 1; start < len(values); start++ {
			sqlBuffer.WriteByte(',')
			sqlBuffer.Write(v)
			for _, fieldName := range dbFieldList {
				*args = append(*args, values[start].FieldByName(fieldName).Interface())
			}
		}
	}

	return sqlBuffer.String(), nil
}

func SqlDeleteByObj(args *[]interface{}, obj interface{}) (sqlStr string, err error) {
	var (
		sqlBuffer strings.Builder
		value     = reflect.ValueOf(obj)
	)

	if value.Kind() != reflect.Ptr {
		return "", ErrInvalidTypes
	}

	value = value.Elem()

	if value.Kind() != reflect.Struct {
		return "", ErrInvalidTypes
	}

	var (
		t          = value.Type()
		fieldCount = value.NumField()

		dbField   string
		isPrimary bool
		where     = make(map[string][]interface{}, 2)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isPrimary = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = "`" + tags[0] + "`"
		for _, val := range tags {
			if strings.TrimSpace(val) == primary {
				isPrimary = true
			}
		}

		if isPrimary {
			where[dbField] = []interface{}{value.Field(i).Interface()}
			continue
		}
	}

	//没有找到主键
	if len(where) < 1 {
		return "", ErrNotFoundPrimaryField
	}

	sqlBuffer.WriteString("DELETE FROM ")
	sqlBuffer.WriteByte('`')
	sqlBuffer.WriteString(tableName(value))
	sqlBuffer.WriteByte('`')
	sqlBuffer.WriteString((Where{AndWhere(AndCondition(where))}).Sql(args))

	return sqlBuffer.String(), nil
}

func SqlUpdateByObj(args *[]interface{}, obj interface{}) (sqlStr string, err error) {
	var (
		sqlBuffer strings.Builder

		value = reflect.ValueOf(obj)
	)

	if value.Kind() != reflect.Ptr {
		return "", ErrInvalidTypes
	}

	if bs := value.MethodByName(beforeSave); bs.Kind() == reflect.Func {
		bs.Call(nil)
	}

	if bc := value.MethodByName(beforeUpdate); bc.Kind() == reflect.Func {
		bc.Call(nil)
	}

	value = value.Elem()
	if value.Kind() != reflect.Struct {
		return "", ErrInvalidTypes
	}

	var (
		t          = value.Type()
		fieldCount = value.NumField()

		dbField     string
		isRequired  bool
		isPrimary   bool
		hasSetField bool
		where       = make(map[string][]interface{}, 2)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isRequired = false
		isPrimary = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = tags[0]
		for _, val := range tags {
			switch strings.TrimSpace(val) {
			case required:
				isRequired = true
			case primary:
				isPrimary = true
			}
		}

		if isPrimary {
			where[dbField] = []interface{}{value.Field(i).Interface()}
			continue
		}

		if !isRequired && value.Field(i).IsZero() {
			continue
		}

		if hasSetField {
			sqlBuffer.WriteByte(',')
		} else {
			hasSetField = true
			sqlBuffer.WriteString("UPDATE ")
			sqlBuffer.WriteByte('`')
			sqlBuffer.WriteString(tableName(value))
			sqlBuffer.WriteByte('`')
			sqlBuffer.WriteString(" SET ")
		}

		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString(dbField)
		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString("=?")
		*args = append(*args, value.Field(i).Interface())
	}

	if !hasSetField {
		return "", ErrNotFoundField
	}

	//没有找到主键
	if len(where) < 1 {
		return "", ErrNotFoundPrimaryField
	}

	sqlBuffer.WriteString((Where{AndWhere(AndCondition(where))}).Sql(args))
	return sqlBuffer.String(), nil
}
