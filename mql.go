package mql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

var DRIVER string

func init() {
	DRIVER = ""
}

func bytesToStr(slc []uint8) string {
	var str string
	for _, c := range slc {
		if c != 34 { //remove "
			str += fmt.Sprintf("%c", c)
		}
	}
	return str
}

func RawSQL(sql string, db *sql.DB) error {
	_, err := db.Exec(sql)
	return err
}

func GetRows2(rows *sql.Rows) ([]map[string]interface{}, error) {

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	var isStarFound bool
	var colCount int
	var orows = make([]map[string]interface{}, 0)

	for rows.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		var orow = make(map[string]interface{})
		for i, col := range columns {
			colCount++
			if col == "*" {
				isStarFound = true
			}
			val := values[i]
			orow[col] = val
		}
		orows = append(orows, orow)
	}

	//process
	var nrows = make([]map[string]interface{}, 0)

	if isStarFound {

		for _, row := range orows {

			for _, col := range columns {
				var vmap = make(map[string]interface{})
				json.Unmarshal(row[col].([]uint8), &vmap)
				for key := range vmap {
					vrow, isOk := vmap[key].(map[string]interface{})
					if isOk {
						nrows = append(nrows, vrow)
					} else {
						fmt.Printf("%v %T\n", vmap[key], vmap[key])
					}
				}
			}
		}

	} else if colCount == 1 {

		for _, row := range orows {
			for _, val := range row {
				json.Unmarshal(val.([]uint8), &row)
			}
			nrows = append(nrows, row)
		}

	} else if colCount > 1 {

		for _, row := range orows {
			var srow = make(map[string]interface{})
			for key, val := range row {
				vbs, isOk := val.([]uint8)
				if isOk {
					srow[key] = bytesToStr(vbs) //?
				} else {
					srow[key] = val
				}
			}
			nrows = append(nrows, srow)
		}
	}
	return nrows, nil
}

func tryUnmarshalFlexible(val interface{}) (map[string]interface{}, bool) {
	var raw []byte

	switch v := val.(type) {
	case []uint8:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return nil, false
	}

	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, false
	}
	return result, true
}

func GetRows1(sql string, db *sql.DB) ([]map[string]interface{}, error) {

	if db == nil {
		return nil, fmt.Errorf("check your dbconnection!")
	}
	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	var isStarFound bool
	var colCount int
	var orows = make([]map[string]interface{}, 0)

	for rows.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		var orow = make(map[string]interface{})
		for i, col := range columns {
			colCount++
			if col == "*" {
				isStarFound = true
			}
			val := values[i]
			orow[col] = val
		}
		orows = append(orows, orow)
	}

	//process
	var nrows = make([]map[string]interface{}, 0)

	if isStarFound {

		for _, row := range orows {

			for _, col := range columns {
				var vmap = make(map[string]interface{})
				json.Unmarshal(row[col].([]uint8), &vmap)
				for key := range vmap {
					vrow, isOk := vmap[key].(map[string]interface{})
					if isOk {
						nrows = append(nrows, vrow)
					} else {
						fmt.Printf("%v %T\n", vmap[key], vmap[key])
					}
				}
			}
		}

	} else if colCount == 1 {

		for _, row := range orows {
			for _, val := range row {
				if parsed, ok := tryUnmarshalFlexible(val); ok {
					nrows = append(nrows, parsed)
				} else {
					nrows = append(nrows, row) // fallback to original row
				}
				break // only need to try one value per row
			}
		}

	} else if colCount > 1 {

		for _, row := range orows {
			var srow = make(map[string]interface{})
			for key, val := range row {
				vbs, isOk := val.([]uint8)
				if isOk {
					srow[key] = bytesToStr(vbs) //?
				} else {
					srow[key] = val
				}
			}
			nrows = append(nrows, srow)
		}
	}
	return nrows, nil
}

func GetRows(query string, db *sql.DB) ([]map[string]interface{}, error) {
	if db == nil {
		return nil, fmt.Errorf("check your database connection")
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column information (do this once)
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Pre-analyze column types (avoid repeated DatabaseTypeName() calls)
	count := len(columns)
	typeHandlers := make([]func([]byte) interface{}, count)
	for i, colType := range columnTypes {
		typeHandlers[i] = getTypeHandler(colType.DatabaseTypeName())
	}

	// Preallocate with estimated capacity
	results := make([]map[string]interface{}, 0, 100)

	// Reusable buffers
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		// Build typed row map
		row := make(map[string]interface{}, count)
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
				continue
			}

			// Fast path: if already proper type
			if b, ok := val.([]uint8); ok {
				row[col] = typeHandlers[i](b)
			} else {
				row[col] = val
			}
		}

		results = append(results, row)
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// getTypeHandler returns a specialized conversion function for each type
func getTypeHandler(dbType string) func([]byte) interface{} {
	switch dbType {
	case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return func(b []byte) interface{} {
			if i, err := strconv.ParseInt(string(b), 10, 64); err == nil {
				return i
			}
			return string(b)
		}

	case "INT UNSIGNED", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
		"MEDIUMINT UNSIGNED", "BIGINT UNSIGNED":
		return func(b []byte) interface{} {
			if u, err := strconv.ParseUint(string(b), 10, 64); err == nil {
				return u
			}
			return string(b)
		}

	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
		return func(b []byte) interface{} {
			if f, err := strconv.ParseFloat(string(b), 64); err == nil {
				return f
			}
			return string(b)
		}

	case "BOOL", "BOOLEAN":
		return func(b []byte) interface{} {
			s := string(b)
			return s == "1" || s == "true"
		}

	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return func(b []byte) interface{} {
			return string(b)
		}

	case "BLOB", "BINARY", "VARBINARY":
		return func(b []byte) interface{} {
			return b // Keep as []byte
		}

	default:
		return func(b []byte) interface{} {
			return string(b)
		}
	}
}

func insertUpdateMapSQL(form map[string]interface{}, db *sql.DB) error {

	id := form["id"] //if update
	todo := form["todo"]
	tableName := fmt.Sprint(form["table"])
	primaryKeyField := form["pkfield"]

	if todo == "" {
		return errors.New("todo is missing")
	}

	dbColList, err := ReadTable2Columns(tableName, db)
	if err != nil {
		return err
	}

	keyAray, valAray := form2KeyValueSliceMap(form, dbColList)
	if todo == "update" {
		whereCondition := fmt.Sprintf("%s='%v'", primaryKeyField, id) //if always id then we may avoid it
		sql := updateQueryBuilder(keyAray, tableName, whereCondition)
		_, err = updateByValAray(sql, valAray, db)
		if err != nil {
			return err
		}
	} else if todo == "insert" {
		sql := insertQueryBuilder(keyAray, tableName) //mysql
		_, _, err := finsert(sql, valAray, db)
		if err != nil {
			return err
		}
	}
	return nil
}

func InsertUpdateMap(form map[string]interface{}, db *sql.DB) error {

	var err error
	if DRIVER == "mysql" || DRIVER == "postgres" {
		err = insertUpdateMapSQL(form, db)
	} else if DRIVER == "n1ql" {
		err = insertUpdateMapNQL(form, db)
	}
	return err
}

// Couchbase
func insertUpdateMapNQL(form map[string]interface{}, db *sql.DB) error {

	modelName, isFound := form["table"].(string) //collection
	if !isFound {
		return fmt.Errorf("collection name missing")
	}
	docID, isFound := form["id"].(string)
	if !isFound {
		return fmt.Errorf("id missing")
	}
	tableName, isFound := form["type"].(string)
	if !isFound {
		tableName = customTableName(modelName)
		form["type"] = tableName
	}

	form2 := structValueProcess(modelName, form) //n1ql
	//jsonTxt := vMapToJsonStr(form2)
	//prepareStatement := upsertQueryBuilder(tableToBucket(tableName), docID, jsonTxt) //
	jsonBytes := vMapToJsonBytes(form2)
	prepareStatement := fmt.Sprintf("UPSERT INTO %s values (?,?)", tableToBucket(tableName))

	stmt, err := db.Prepare(prepareStatement)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(docID, jsonBytes) //_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}
