package sqlstream

import (
	"context"
	"database/sql"

	"github.com/suifengpiao14/stream"
)

type GetDBI interface {
	GetDB() *sql.DB
}

// MysqlPackHandler 执行sql获取返回
func MysqlPackHandler(db *sql.DB) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		data, err := ExecOrQueryContext(ctx, db, sql)
		if err != nil {
			return nil, err
		}
		return []byte(data), nil
	}, nil)
	return packHandler
}
