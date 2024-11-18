package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDB struct {
	*sql.DB
}

func New(dsn string) (*MySQLDB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &MySQLDB{db}, nil
}
