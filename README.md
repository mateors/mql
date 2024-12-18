# mql
Combining MySQL/MariaDB, PostgreSQL, and Couchbase into a single package.

### How to install

> go get github.com/mateors/mql


### Couchbase database Read code sample

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/mateors/mql"
	"github.com/mateors/mql/database/couchbase"
)

var DBUSER, DBPASS, HOST, DBPORT, DBNAME, CHARSET, DRIVER string
var db *sql.DB
var err error

// Model = A model is like a SQL table.
type Login struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	CompanyID          string `json:"cid"`          //foreign key
	AccountID          string `json:"account_id"`   //foreign key
	AccessID           string `json:"access_id"`    //foreign key
	AccessName         string `json:"access_name"`  //customer type
	UserName           string `json:"username"`     //email or mobile as username
	Password           string `json:"passw"`        //password
	TfaStatus          int    `json:"tfa_status"`   //TFA = 0,1
	TfaMedium          string `json:"tfa_medium"`   //TFA
	TfaSetupkey        string `json:"tfa_setupkey"` //TFA
	IpAddress          string `json:"ip_address"`   //last login ip
	IncorrectPassCount int    `json:"ipcount"`      //the number of sign-in attempts with incorrect passwords.
	CreateDate         string `json:"create_date"`
	UpdateDate         string `json:"update_date"`
	LastLogin          string `json:"last_login,omitempty"` //update date
	Status             int    `json:"status"`
}

func init() {

	DRIVER = "n1ql"
	DBUSER = "lxrtestusr"
	DBPASS = "B@nglade$h2.0"
	HOST = "localhost"
	DBPORT = "8093"
	DBNAME = "lxroot" //Bucket

	// Couchbase database connection string
	dataSourceName := fmt.Sprintf("http://%s:%s@%s:%s", DBUSER, DBPASS, HOST, DBPORT)
	pdb, err := couchbase.New(dataSourceName)
	if err != nil {
		fmt.Println("Error connecting to Couchbase:", err)
		return
	}
	db = pdb.DB
}

func main() {

	defer db.Close()

	mql.DRIVER = DRIVER
	mql.BUCKET = DBNAME        //
	mql.SCOPE = "_default"     //
	mql.RegisterModel(Login{}) //

	rows, err := db.Query("SELECT id,username FROM lxroot._default.login;")
	if err != nil {
		fmt.Println("Query error:", err)
		return
	}
	defer rows.Close()
	nrows, err := mql.GetRows(rows)
	fmt.Println(err)
	if err != nil {
		return
	}
	for i, row := range nrows {
		fmt.Println(i, row)
	}
}

```

## Couchbase database Insert/Update code sample

```go
package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/mateors/mql"
	"github.com/mateors/mql/database/couchbase"
)

var DBUSER, DBPASS, HOST, DBPORT, DBNAME, CHARSET, DRIVER string
var db *sql.DB
var err error

// Model = A model is like a SQL table.
type Login struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	CompanyID          string `json:"cid"`          //foreign key
	AccountID          string `json:"account_id"`   //foreign key
	AccessID           string `json:"access_id"`    //foreign key
	AccessName         string `json:"access_name"`  //customer type
	UserName           string `json:"username"`     //email or mobile as username
	Password           string `json:"passw"`        //password
	TfaStatus          int    `json:"tfa_status"`   //TFA = 0,1
	TfaMedium          string `json:"tfa_medium"`   //TFA
	TfaSetupkey        string `json:"tfa_setupkey"` //TFA
	IpAddress          string `json:"ip_address"`   //last login ip
	IncorrectPassCount int    `json:"ipcount"`      //the number of sign-in attempts with incorrect passwords.
	CreateDate         string `json:"create_date"`
	UpdateDate         string `json:"update_date"`
	LastLogin          string `json:"last_login,omitempty"` //update date
	Status             int    `json:"status"`
}

func init() {

	DRIVER = "n1ql"
	DBUSER = "lxrtestusr"
	DBPASS = "B@nglade$h2.0"
	HOST = "localhost"
	DBPORT = "8093"
	DBNAME = "lxroot" //Bucket

	// Couchbase database connection string
	dataSourceName := fmt.Sprintf("http://%s:%s@%s:%s", DBUSER, DBPASS, HOST, DBPORT)
	pdb, err := couchbase.New(dataSourceName)
	if err != nil {
		fmt.Println("Error connecting to Couchbase:", err)
		return
	}
	db = pdb.DB
}

func main() {

	defer db.Close()

	mql.DRIVER = DRIVER
	mql.BUCKET = DBNAME        //
	mql.SCOPE = "_default"     //
	mql.RegisterModel(Login{}) //

	timeNow := time.Now().Format("2006-01-02 15:04:05")
	form := make(map[string]interface{}, 0)
	form["todo"] = "insert" //update
	form["table"] = "Login" //Login == Model
	form["id"] = "id05"
	form["cid"] = 1
	form["access_name"] = "admin"
	form["username"] = "kaunia"
	form["passw"] = "test321"
	form["account_id"] = "cjmp118jn6o23j2ild40"
	form["create_date"] = timeNow
	form["status"] = 1
	err = mql.InsertUpdateMap(form, db)
	fmt.Println(err)
}

```

## Coucbase database Delete sample code

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/mateors/mql"
	"github.com/mateors/mql/database/couchbase"
)

var DBUSER, DBPASS, HOST, DBPORT, DBNAME, CHARSET, DRIVER string
var db *sql.DB
var err error

// Model = A model is like a SQL table.
type Login struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	CompanyID          string `json:"cid"`          //foreign key
	AccountID          string `json:"account_id"`   //foreign key
	AccessID           string `json:"access_id"`    //foreign key
	AccessName         string `json:"access_name"`  //customer type
	UserName           string `json:"username"`     //email or mobile as username
	Password           string `json:"passw"`        //password
	TfaStatus          int    `json:"tfa_status"`   //TFA = 0,1
	TfaMedium          string `json:"tfa_medium"`   //TFA
	TfaSetupkey        string `json:"tfa_setupkey"` //TFA
	IpAddress          string `json:"ip_address"`   //last login ip
	IncorrectPassCount int    `json:"ipcount"`      //the number of sign-in attempts with incorrect passwords.
	CreateDate         string `json:"create_date"`
	UpdateDate         string `json:"update_date"`
	LastLogin          string `json:"last_login,omitempty"` //update date
	Status             int    `json:"status"`
}

func init() {

	DRIVER = "n1ql"
	DBUSER = "lxrtestusr"
	DBPASS = "B@nglade$h2.0"
	HOST = "localhost"
	DBPORT = "8093"
	DBNAME = "lxroot" //Bucket

	// Couchbase database connection string
	dataSourceName := fmt.Sprintf("http://%s:%s@%s:%s", DBUSER, DBPASS, HOST, DBPORT)
	pdb, err := couchbase.New(dataSourceName)
	if err != nil {
		fmt.Println("Error connecting to Couchbase:", err)
		return
	}
	db = pdb.DB
}

func main() {

	defer db.Close()

	mql.DRIVER = DRIVER
	mql.BUCKET = DBNAME        //
	mql.SCOPE = "_default"     //
	mql.RegisterModel(Login{}) //

	err = mql.RawSQL("DELETE FROM lxroot._default.login WHERE id='id05';", db)
	fmt.Println(err)
}

```
