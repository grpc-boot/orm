package orm

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"

	"github.com/grpc-boot/base"
)

type RowFormat func(fieldValue map[string][]byte)

func FormatRows(rows *sql.Rows, handler RowFormat) {
	fields, err := rows.Columns()
	if err != nil {
		return
	}

	if len(fields) == 0 {
		return
	}

	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return
		}

		row := make(map[string][]byte, len(fields))
		for index, field := range fields {
			row[field] = *values[index].(*[]byte)
		}

		handler(row)
	}
}

func ToMap(rows *sql.Rows) ([]map[string]string, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, nil
	}

	var data []map[string]string
	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]string, len(fields))
		for index, field := range fields {
			row[field] = base.Bytes2String(*values[index].(*[]byte))
		}
		data = append(data, row)
	}

	return data, nil
}

func ToObj(rows *sql.Rows, obj interface{}) error {
	fields, err := rows.Columns()
	if err != nil {
		return err
	}

	if len(fields) == 0 {
		return nil
	}

	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	row := make(map[string][]byte, len(fields))
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		for index, field := range fields {
			row[field] = *values[index].(*[]byte)
		}
	}

	if len(row) < 1 {
		return nil
	}

	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return ErrInvalidTypes
	}

	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return ErrInvalidTypes
	}

	var (
		fieldCount = v.NumField()
	)

	if fieldCount < 1 {
		return nil
	}

	var (
		t = v.Type()
	)

	for i := 0; i < fieldCount; i++ {
		tag := t.Field(i).Tag.Get(tagName)
		if tag == "" {
			continue
		}

		fieldName := strings.Split(tag, ",")[0]
		if _, exists := row[fieldName]; !exists {
			continue
		}

		switch v.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.Field(i).SetInt(base.Bytes2Int64(row[fieldName]))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v.Field(i).SetUint(uint64(base.Bytes2Int64(row[fieldName])))
		case reflect.String:
			v.Field(i).SetString(base.Bytes2String(row[fieldName]))
		case reflect.Float32, reflect.Float64:
			var val float64
			val, err = strconv.ParseFloat(base.Bytes2String(row[fieldName]), 64)
			if err != nil {
				continue
			}
			v.Field(i).SetFloat(val)
		case reflect.Bool:
			v.Field(i).SetBool(base.Bytes2String(row[fieldName]) == "1")
		default:
			return ErrInvalidFieldTypes
		}
	}

	return nil
}
