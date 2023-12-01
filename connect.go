package sqlexec

import (
	"database/sql"
	"sync"

	"github.com/pkg/errors"
)

type GetDBI interface {
	GetDB() *sql.DB
}

var dbMap sync.Map

func RegisterDB(identity string, db *sql.DB) (err error) {
	dbMap.Store(identity, db)
	return nil
}

func GetDB(identify string) (db *sql.DB, err error) {
	val, ok := dbMap.Load(identify)
	if !ok {
		err = errors.Errorf("not found db by identify:%s,use RegisterDB to set", identify)
		return nil, err
	}
	p, ok := val.(*sql.DB)
	if !ok {
		err = errors.Errorf("required:%v,got:%v", &sql.DB{}, val)
		return nil, err
	}
	return p, nil
}
