package mql

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

var typeRegistry = make(map[string]reflect.Type)

var (
	BUCKET, SCOPE string
)

type structFieldDetails struct {
	FiledName string
	TagName   string
	Type      string
	OmiyEmpty bool
}

type charInfo struct {
	Index int
	Char  rune
}

func RegisterModel(emptyStruct interface{}) {
	typeRegistry[reflect.TypeOf(emptyStruct).Name()] = reflect.TypeOf(emptyStruct)
}

func upperCount(text string) []*charInfo {

	var list []*charInfo
	for i, ch := range text {
		if unicode.IsUpper(ch) {
			list = append(list, &charInfo{i, ch})
		}
	}
	return list
}

func splitByUpperCase(text string) []string {

	var list []string
	ci := upperCount(text)
	splitIndex := 0
	for i, c := range ci {
		if i > 0 {
			list = append(list, text[splitIndex:c.Index])
			splitIndex = c.Index
		}
	}
	if splitIndex < len(text) {
		list = append(list, text[splitIndex:])
	}
	return list
}

func customTableName(structName string) string {

	list := splitByUpperCase(structName)
	for i, part := range list {
		list[i] = strings.ToLower(part)
	}
	return strings.Join(list, "_")
}

func makeInstance(name string) interface{} {
	rval, isFound := typeRegistry[name]
	if !isFound {
		return nil
	}
	return reflect.New(rval).Elem().Interface()
}

func structNameToFields(structName string) (fieldList []*structFieldDetails) {

	defer func() {
		if r := recover(); r != nil {
			log.Println("was panic, recovered value", r)
			fieldList = nil
		}
	}()

	sInstance := makeInstance(structName)
	iVal := reflect.ValueOf(sInstance)
	iTypeOf := iVal.Type()

	for i := 0; i < iVal.NumField(); i++ {

		typeName := iTypeOf.Field(i).Type.String()
		fieldName := iTypeOf.Field(i).Name
		fieldTag := iTypeOf.Field(i).Tag.Get("json")
		var omitFound bool
		if strings.Contains(fieldTag, ",") {
			omitFound = true
		}
		if omitFound {
			commaFoundAt := strings.Index(fieldTag, ",")
			ntag := fieldTag[0:commaFoundAt]
			fieldList = append(fieldList, &structFieldDetails{fieldName, ntag, typeName, omitFound})
		} else {
			fieldList = append(fieldList, &structFieldDetails{fieldName, fieldTag, typeName, omitFound})
		}
	}
	return fieldList
}

func structValueProcess(structName string, form map[string]interface{}) map[string]interface{} {

	var rform = make(map[string]interface{})
	fslc := structNameToFields(structName) //

	for _, fd := range fslc {

		val, isParsed := form[fd.TagName]
		if !isParsed {
			val = ""
		}
		if fd.Type == "int" {
			kval, _ := strconv.Atoi(fmt.Sprint(val))
			rform[fd.TagName] = kval

		} else if fd.Type == "int64" {
			kval, _ := strconv.ParseInt(fmt.Sprint(val), 10, 64)
			rform[fd.TagName] = kval

		} else if fd.Type == "float64" {
			kval, _ := strconv.ParseFloat(fmt.Sprint(val), 64)
			rform[fd.TagName] = kval

		} else if fd.Type == "string" {
			rform[fd.TagName] = val

		} else {
			rform[fd.TagName] = form[fd.TagName]
		}
	}
	return rform
}

func vMapToJsonStr(vMap map[string]interface{}) string {
	bs, err := json.Marshal(&vMap)
	if err != nil {
		return ""
	}
	return string(bs)
}
func vMapToJsonBytes(vMap map[string]interface{}) []byte {
	bs, err := json.Marshal(&vMap)
	if err != nil {
		return nil
	}
	return bs
}

func upsertQueryBuilder(bucketName, docID, bytesTxt string) (nqlStatement string) {
	qs := `UPSERT INTO %s (KEY, VALUE)
	VALUES ("%s", %s)
	RETURNING *`
	nqlStatement = fmt.Sprintf(qs, bucketName, docID, bytesTxt)
	return
}

func tableToBucket(table string) string {
	return fmt.Sprintf(`%s.%s.%s`, BUCKET, SCOPE, table)
}
