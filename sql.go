package mql

import (
	"database/sql"
	"fmt"
	"strings"
)

func ReadTable2Columns(table string, db *sql.DB) ([]string, error) {

	if DRIVER == "postgres" {
		return readTable2ColumnsPSQL(table, db)
	} else if DRIVER == "mysql" {
		return ReadTable2Columns(table, db)
	}
	return nil, fmt.Errorf("unknown driver")
}

func readTable2Columns(table string, db *sql.DB) ([]string, error) {

	sqls := fmt.Sprintf("SHOW COLUMNS FROM `%v`;", table)
	rows, err := db.Query(sqls)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	//var vfield, vtype, vnull, vkey, vextra string
	var vfield, vtype, vnull, vkey, vextra sql.NullString
	var vdefault *string

	cols := []string{}
	for rows.Next() {
		err = rows.Scan(&vfield, &vtype, &vnull, &vkey, &vdefault, &vextra)
		if err != nil {
			return nil, err
		}
		if vfield.Valid {
			//val, _ := vfield.Value()
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

	//var vfield, vtype, vnull, vkey, vextra string
	var vfield, vtype, vnull, vkey, vextra sql.NullString
	var vdefault *string

	cols := []string{}
	for rows.Next() {
		err = rows.Scan(&vfield, &vtype, &vnull, &vkey, &vdefault, &vextra)
		if err != nil {
			return nil, err
		}
		if vfield.Valid {
			//val, _ := vfield.Value()
			cols = append(cols, vfield.String)
		}
	}
	return cols, nil
}

func form2KeyValueSliceMap(form map[string]interface{}, colList []string) (keyList []string, valList []string) {

	fmap := make(map[string]string)
	for key, valAray := range form {
		//val := valAray
		fmap[key] = fmt.Sprint(valAray)
	}

	for _, colName := range colList {

		var cval = ""
		if colval, ok := fmap[colName]; ok {
			//fmt.Printf("%v-> %v exist value = %v\n", i, colName, colval)
			cval = colval
		} else {
			//fmt.Printf("%v-> %v NOT IN MAP => %v\n", i, colName, colval)
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
		//fields += fmt.Sprintf("%v=?, ", v)
		fields += fmt.Sprintf("%v=$%d, ", v, i+1)
	}
	fmt.Fprintf(sb, "UPDATE %v SET %v WHERE %v;", tableName, strings.TrimRight(fields, ", "), whereCondition)
	sql = sb.String()
	return
}

func updateQueryBuilder(keyVal []string, tableName string, whereCondition string) string {

	sql := updateQueryBuilderMSQL(keyVal, tableName, whereCondition)
	if DRIVER == "postgres" {
		sql = updateQueryBuilderPSQL(keyVal, tableName, whereCondition)
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
			//fields += fmt.Sprintf("%v, ", v)
			fields += "NULL, "
		} else {
			//fields += fmt.Sprintf("`%v`, ", v) //mysql
			fields += fmt.Sprintf("%v, ", v) //postgresql
		}

		vals += "?, "
	}
	//fmt.Fprintf(sb, "INSERT INTO `%v` (%v) VALUES(%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	fmt.Fprintf(sb, "INSERT INTO %v (%v) VALUES (%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	sqlstr := sb.String()
	fmt.Println(sqlstr)
	return sqlstr
}

func insertQueryBuilderPSQL(keyVal []string, tableName string) string {

	sb := &strings.Builder{}
	fields := ""
	vals := ""

	//ignoring slice 0 index value which is primary key auto incremented
	for i, v := range keyVal {

		if v == "NULL" {
			//fields += fmt.Sprintf("%v, ", v)
			fields += "NULL, "
		} else {
			//fields += fmt.Sprintf("`%v`, ", v) //mysql
			fields += fmt.Sprintf("%v, ", v) //postgresql
		}

		//vals += "?, "
		vals += fmt.Sprintf("$%d, ", i+1)
	}
	//fmt.Fprintf(sb, "INSERT INTO `%v` (%v) VALUES(%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	fmt.Fprintf(sb, "INSERT INTO %v (%v) VALUES (%v);", tableName, strings.TrimRight(fields, ", "), strings.TrimRight(vals, ", "))
	sqlstr := sb.String()
	fmt.Println(sqlstr)
	return sqlstr
}

func insertQueryBuilder(keyVal []string, tableName string) string {

	qstr := ""
	if DRIVER == "postgres" {
		qstr = insertQueryBuilderPSQL(keyVal, tableName)
	} else if DRIVER == "mysql" {
		qstr = insertQueryBuilderMSQL(keyVal, tableName)
	}
	return qstr
}

func finsert(sql string, valAray []string, db *sql.DB) (int64, int64, error) {

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
	lrid, _ := res.LastInsertId()
	lcount, _ := res.RowsAffected()
	return lrid, lcount, nil
}
