package sqlexec

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

var tablePool sync.Map

func getTablePoolKey(database string, tableName string) (key string) {
	return fmt.Sprintf("%s_%s", database, tableName)
}
func RegisterTable(database string, tables ...sqlexecparser.Table) {
	for _, table := range tables {
		key := getTablePoolKey(database, table.TableName)
		tablePool.Store(key, &table)
	}
}

var (
	ERROR_NOT_FOUND_TABLE = errors.New("not found table")
	ERROR_INVALID_TYPE    = errors.New("invalid type, except *parser.Table")
)

func GetTable(database string, tableName string) (table *sqlexecparser.Table, err error) {
	key := getTablePoolKey(database, tableName)
	v, ok := tablePool.Load(key)
	if !ok {
		err = errors.WithMessagef(ERROR_NOT_FOUND_TABLE, "%s", key)
		return nil, err
	}
	table, ok = v.(*sqlexecparser.Table)
	if !ok {
		return nil, ERROR_INVALID_TYPE
	}
	return table, nil
}

// RegisterTableByDDL 通过ddl语句注册表结构,避免依赖db连接,方便本地化启动模块
func RegisterTableByDDL(ddlStatements string) (err error) {
	tables, err := sqlexecparser.ParseDDL(ddlStatements)
	if err != nil {
		return err
	}
	for dbName, tabs := range tables.GroupByDBName() {
		RegisterTable(dbName, tabs...)
	}
	return nil
}

//GetDBNameFromDSN 从DB 的dsn中获取数据库名称
func GetDBNameFromDSN(dsn string) (string, error) {
	// 使用正则表达式提取数据库名称
	re := regexp.MustCompile(`\/([^\/\?]+)`)
	matches := re.FindStringSubmatch(dsn)

	if len(matches) < 2 {
		return "", fmt.Errorf("unable to extract database name from DSN")
	}

	return matches[1], nil
}
