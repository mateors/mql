package mql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func ReadTable2Columns(table string, db *sql.DB) ([]string, error) {

	if DRIVER == "postgres" {
		return readTable2ColumnsPSQL(table, db)
	} else if DRIVER == "mysql" {
		return readTable2ColumnsMSQL(table, db)
	} else if DRIVER == "n1ql" {
		return readTable2ColumnsNQL(table)
	}
	return nil, fmt.Errorf("unknown driver")
}

func readTable2ColumnsMSQL(table string, db *sql.DB) ([]string, error) {

	sqls := fmt.Sprintf("SHOW COLUMNS FROM `%v`;", table)
	rows, err := db.Query(sqls)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vfield, vtype, vnull, vkey, vextra sql.NullString
	var vdefault *string
	cols := []string{}

	for rows.Next() {
		err = rows.Scan(&vfield, &vtype, &vnull, &vkey, &vdefault, &vextra)
		if err != nil {
			return nil, err
		}
		if vfield.Valid {
			cols = append(cols, vfield.String)
		}
	}
	return cols, nil
}

func readTable2ColumnsPSQL(table string, db *sql.DB) ([]string, error) {

	qs := `SELECT 
    column_name AS "Field",
    data_type AS "Type",
    CASE WHEN is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Null",
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND kcu.table_name = c.table_name
            AND kcu.column_name = c.column_name
        ) THEN 'PRI' 
        ELSE NULL 
    END AS "Key",
    column_default AS "Default",
    '' AS "Extra"
FROM 
    information_schema.columns c
WHERE 
    table_name = '%s';`
	sqls := fmt.Sprintf(qs, table)
	rows, err := db.Query(sqls)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var vfield, vtype, vnull, vkey, vextra sql.NullString
	var vdefault *string

	cols := []string{}
	for rows.Next() {
		err = rows.Scan(&vfield, &vtype, &vnull, &vkey, &vdefault, &vextra)
		if err != nil {
			return nil, err
		}
		if vfield.Valid {
			cols = append(cols, vfield.String)
		}
	}
	return cols, nil
}

func readTable2ColumnsNQL(table string) ([]string, error) {

	cols := []string{}
	fList := structNameToFields(table)
	for _, fld := range fList {
		cols = append(cols, fld.TagName)
	}
	return cols, nil
}

func form2KeyValueSliceMap(form map[string]interface{}, colList []string) (keyList []string, valList []string) {

	fmap := make(map[string]string)
	for key, valAray := range form {
		fmap[key] = fmt.Sprint(valAray)
	}

	for _, colName := range colList {

		var cval = ""
		if colval, ok := fmap[colName]; ok {
			cval = colval
		}
		if cval != "" {
			keyList = append(keyList, colName)
			valList = append(valList, cval)
		}
	}
	return
}

func updateQueryBuilderMSQL(keyVal []string, tableName string, whereCondition string) (sql string) {

	sb := &strings.Builder{}
	var fields string
	for _, v := range keyVal {
		fields += fmt.Sprintf("`%v`=?, ", v)
	}
	fmt.Fprintf(sb, "UPDATE `%v` SET %v WHERE %v;", tableName, strings.TrimRight(fields, ", "), whereCondition)
	sql = sb.String()
	return
}

func updateQueryBuilderPSQL(keyVal []string, tableName string, whereCondition string) (sql string) {

	sb := &strings.Builder{}
	var fields string
	for i, v := range keyVal {
		fields += fmt.Sprintf("%v=$%d, ", v, i+1)
	}
	fmt.Fprintf(sb, "UPDATE %v SET %v WHERE %v;", tableName, strings.TrimRight(fields, ", "), whereCondition)
	sql = sb.String()
	return
}

func updateQueryBuilder(keyVal []string, tableName string, whereCondition string) string {

	var sql string
	if DRIVER == "postgres" {
		sql = updateQueryBuilderPSQL(keyVal, tableName, whereCondition)
	} else if DRIVER == "mysql" {
		sql = updateQueryBuilderMSQL(keyVal, tableName, whereCondition)
	}
	return sql
}

func updateByValAray(sql string, valAray []string, db *sql.DB) (rowsAfftected int64, err error) {

	stmt, err := db.Prepare(sql)
	if err != nil {
		return 0, err
	}

	defer stmt.Close()
	v := make([]interface{}, len(valAray))
	for i, val := range valAray {
		v[i] = val
	}

	res, err := stmt.Exec(v...)
	if err != nil {
		return 0, err
	}
	rowsAfftected, _ = res.RowsAffected()
	return
}

func insertQueryBuilderMSQL(keyVal []string, tableName string) string {

	sb := &strings.Builder{}
	fields := ""
	vals := ""
	//ignoring slice 0 index value which is primary key auto incremented
	for _, v := range keyVal {
		if v == "NULL" {
			fields += "NULL, "
		} else {
			fields += fmt.Sprintf("%v, ", v)
		}
		vals += "?, "
	}
	fmt.Fprintf(sb, "INSERT INTO %v (%v) VALUES (%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	return sb.String()
}

func insertQueryBuilderPSQL(keyVal []string, tableName string) string {

	sb := &strings.Builder{}
	fields := ""
	vals := ""
	//ignoring slice 0 index value which is primary key auto incremented
	for i, v := range keyVal {
		if v == "NULL" {
			fields += "NULL, "
		} else {
			fields += fmt.Sprintf("%v, ", v)
		}
		vals += fmt.Sprintf("$%d, ", i+1)
	}
	fmt.Fprintf(sb, "INSERT INTO %v (%v) VALUES (%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	return sb.String()
}

func insertQueryBuilder(keyVal []string, tableName string) string {

	var qstr string
	if DRIVER == "postgres" {
		qstr = insertQueryBuilderPSQL(keyVal, tableName)
	} else if DRIVER == "mysql" {
		qstr = insertQueryBuilderMSQL(keyVal, tableName)
	}
	return qstr
}

func finsert(sql string, valAray []string, db *sql.DB) (lastInserId int64, rowAffected int64, err error) {

	stmt, err := db.Prepare(sql)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()
	v := make([]interface{}, len(valAray))
	for i, val := range valAray {
		v[i] = val
	}
	res, err := stmt.Exec(v...)
	if err != nil {
		return 0, 0, err
	}
	lastInserId, err = res.LastInsertId()
	rowAffected, err = res.RowsAffected()
	return
}

func FieldByValue(table, fieldName, where string, db *sql.DB) string {

	var val string
	if DRIVER == "mysql" || DRIVER == "postgres" {
		val = fieldByValueSQL(table, fieldName, where, db)
	} else if DRIVER == "n1ql" {
		val = fieldByValueNQL(table, fieldName, where, db)
	}
	return val
}

func fieldByValueSQL(table, fieldName, where string, db *sql.DB) string {

	var sql string
	if DRIVER == "mysql" {
		sql = fmt.Sprintf("SELECT %v FROM `%v` WHERE %v;", fieldName, table, where)
	} else if DRIVER == "postgres" {
		sql = fmt.Sprintf("SELECT %v FROM `%v` WHERE %v;", fieldName, table, where)
	}
	rows := db.QueryRow(sql)
	var vfield string
	err := rows.Scan(&vfield)
	if err != nil {
		return vfield
	}
	return vfield
}

func scanMap(jsonBytes []uint8) map[string]interface{} {

	var cmap = make(map[string]interface{}, 0)
	err := json.Unmarshal(jsonBytes, &cmap)
	if err != nil {
		return nil
	}
	return cmap
}

// FieldByValue Get one field_value using where clause
func fieldByValueNQL(table, fieldName, where string, db *sql.DB) string {

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s;", fieldName, tableToBucket(table), where)
	rows := db.QueryRow(sql)
	var vfield string
	var jsonBytes []uint8
	err := rows.Scan(&jsonBytes)
	if err != nil {
		return ""
	}
	cmap := scanMap(jsonBytes)
	if len(cmap) > 0 {
		vfield = fmt.Sprint(cmap[fieldName])
	}
	return vfield
}
