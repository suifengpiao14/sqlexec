package sqlstream

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/stream"
)

//DBPackHandlersWithSetContent 将DB打包到数据流上下文中
// func DBPackHandlersWithSetContent(db *sql.DB) (packHandler stream.PackHandler) {
// 	packHandler = stream.NewPackHandlerWithSetContext(func(ctx context.Context, input []byte) (newCtx context.Context, out []byte, err error) {
// 		newCtx = setDBToContext(ctx, db)
// 		return newCtx, input, nil
// 	}, nil, nil)
// 	return packHandler
// }

//MysqlPackHandler 执行sql获取返回
func MysqlPackHandler(db *sql.DB) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandlerWithSetContext(nil, func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		// if db == nil {
		// 	db, _ = GetDBFromContext(ctx)
		// }
		if db == nil {
			err = errors.New("db required")
			return nil, err
		}
		data, err := ExecOrQueryContext(ctx, db, sql)
		if err != nil {
			return nil, err
		}
		return []byte(data), nil
	}, nil)
	return packHandler
}

//Byte2StructPackHandler 将mysql数据转换为结构体
func Byte2StructPackHandler(dst any) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandler(nil, func(ctx context.Context, input []byte) (out []byte, err error) {
		err = Byte2Struct(input, dst)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return packHandler
}
