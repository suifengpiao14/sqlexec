package sqlexecparser

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/pkg/errors"
)

var tablePool sync.Map

type TablePoolKey struct {
	DBName    string `json:"DBName"`
	TableName string `json:"TableName"`
}

func getTablePoolKey(database DBName, tableName TableName) (key TablePoolKey) {

	return TablePoolKey{
		DBName:    database.Base(),
		TableName: tableName.Base(),
	}
}
func RegisterTable(database DBName, tables ...Table) {
	for _, table := range tables {
		key := getTablePoolKey(database, TableName(table.TableName))
		cp := table //此处必须重新赋值，再取地址，否则会编程引用同一个变量
		tablePool.Store(key, &cp)
	}
}

var (
	ERROR_NOT_FOUND_TABLE = errors.New("not found table")
	ERROR_INVALID_TYPE    = errors.New("invalid type, except *parser.Table")
)

func GetTable(database DBName, tableName TableName) (table *Table, err error) {
	key := getTablePoolKey(database, tableName)
	v, ok := tablePool.Load(key)
	if !ok {
		err = errors.WithMessagef(ERROR_NOT_FOUND_TABLE, "%s", key)
		return nil, err
	}
	table, ok = v.(*Table)
	if !ok {
		return nil, ERROR_INVALID_TYPE
	}
	return table, nil
}

// RegisterTableByDDL 通过ddl语句注册表结构,避免依赖db连接,方便本地化启动模块
func RegisterTableByDDL(ddlStatements string) (err error) {
	tables, err := ParseDDL(ddlStatements)
	if err != nil {
		return err
	}
	m := tables.GroupByDBName()
	for dbName, tabs := range m {
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
