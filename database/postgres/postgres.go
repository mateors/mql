package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	*sql.DB
}

func New(dsn string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresDB{db}, nil
}
