package orm

import "database/sql"

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
			row[field] = string(*values[index].(*[]byte))
		}
		data = append(data, row)
	}

	return data, nil
}
