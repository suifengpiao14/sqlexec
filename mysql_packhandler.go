package sqlstream

import (
	"context"
	"database/sql"

	"github.com/suifengpiao14/stream"
)

type GetDBI interface {
	GetDB() *sql.DB
}

type _MysqlPacketHandler struct {
	db *sql.DB
}

func NewMysqlPacketHandler(db *sql.DB) (packHandler stream.PacketHandlerI) {
	return &_MysqlPacketHandler{
		db: db,
	}
}

func (packet *_MysqlPacketHandler) Name() string {
	return stream.GeneratePacketHandlerName(packet)
}

func (packet *_MysqlPacketHandler) Description() string {
	return `执行sql获取数据,并输出json格式数据,数据中字段类型全部设置为string类型`
}
func (packet *_MysqlPacketHandler) Before(ctx context.Context, input []byte) (newCtx context.Context, out []byte, err error) {
	sql := string(input)
	data, err := ExecOrQueryContext(ctx, packet.db, sql)
	if err != nil {
		return ctx, nil, err
	}
	return ctx, []byte(data), nil
}
func (packet *_MysqlPacketHandler) After(ctx context.Context, input []byte) (newCtx context.Context, out []byte, err error) {
	return ctx, input, nil
}

func (packet *_MysqlPacketHandler) String() string {
	return ""
}
