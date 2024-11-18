package couchbase

import (
	"database/sql"

	_ "github.com/mateors/lxcb"
)

type CouchbaseDB struct {
	*sql.DB
}

func New(dsn string) (*CouchbaseDB, error) {
	db, err := sql.Open("n1ql", dsn)
	if err != nil {
		return nil, err
	}
	return &CouchbaseDB{db}, nil
}
