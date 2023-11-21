package sqlstream

import (
	"context"

	"github.com/suifengpiao14/stream"
)

type Identity interface {
	Identity() string
}

// MysqlPackHandler 执行sql获取返回
func MysqlPackHandler(dbIdentity Identity) (packHandler stream.PackHandler) {
	packHandler = stream.NewPackHandlerWithSetContext(nil, func(ctx context.Context, input []byte) (out []byte, err error) {
		sql := string(input)
		db, err := GetDB(dbIdentity.Identity())
		if err != nil {
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

// Byte2StructPackHandler 将mysql数据转换为结构体
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
