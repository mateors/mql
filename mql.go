package mql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

func GetRows(sql string, db *sql.DB) ([]map[string]interface{}, error) {

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
