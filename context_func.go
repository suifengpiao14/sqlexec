package sqlstream

import (
	"github.com/pkg/errors"
)

type contextKey string

var (
	DatabaseKey           contextKey = "databaseKey"
	CONTEXT_NOT_FOUND_KEY            = errors.New("not found key from context")
	CONTEXT_NOT_EXCEPT               = errors.New("not except type")
)

/*
func setDBToContext(ctx context.Context, db *sql.DB) (newCtx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	newCtx = context.WithValue(ctx, DatabaseKey, db)
	return newCtx
}

//GetDBFromContext 从上下文中获取数据库实例
func GetDBFromContext(ctx context.Context) (db *sql.DB, err error) {
	value := ctx.Value(DatabaseKey)
	if value == nil {
		err = errors.WithMessagef(CONTEXT_NOT_FOUND_KEY, "key:%s", DatabaseKey)
		return nil, err
	}
	db, ok := value.(*sql.DB)
	if !ok {
		err = errors.WithMessagef(CONTEXT_NOT_EXCEPT, "except:string,got:%T", value)
		return nil, err
	}
	return db, nil
}
*/
