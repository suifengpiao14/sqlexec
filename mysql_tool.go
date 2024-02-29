package sqlexec

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

//GetDatabaseName 通过DB连接，获取数据库名称，目前支持Mysql
func GetDatabaseName(db *sql.DB) (database string, err error) {
	sql := `SELECT DATABASE() AS current_database;`
	ctx := context.Background()
	row := db.QueryRowContext(ctx, sql)
	err = row.Err()
	if err != nil {
		return "", err
	}
	err = row.Scan(&database)
	if err != nil {
		return "", err
	}
	return database, nil
}

//DDL_Cache_File_Format 从db中获取ddl后存储到文件中，提升初始化效率
var DDL_Cache_File_Format = fmt.Sprintf("%s/%s", os.TempDir(), "ddl/%s.sql")

func GetDDLCache(filename string) (ddl string, err error) {
	ok, err := funcs.FileExists(filename)
	if err != nil {
		return "", err
	}
	if ok {
		b, err := os.ReadFile(filename)
		if err != nil {
			return "", err
		}
		ddl = string(b)
	}
	if ddl == "" {
		err = errors.Errorf("empty file")
		return "", err
	}
	return ddl, nil
}

func SetDDLCache(filename string, ddl string) (err error) {
	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.WriteFile(filename, []byte(ddl), os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

//GetDDL 通过DB连接，获取数据库DDL
func GetDDL(db *sql.DB) (ddl string, err error) {
	arr := make([]string, 0)
	database, err := GetDatabaseName(db)
	if err != nil {
		return "", err
	}
	cacheFilename := fmt.Sprintf(DDL_Cache_File_Format, database)
	ddl, err = GetDDLCache(cacheFilename)
	if err == nil { // 使用缓存
		return ddl, nil
	}
	err = nil
	sql := fmt.Sprintf(sqlexecparser.Create_DB_SQL_Format, database) //增加建库语句
	arr = append(arr, sql)
	tables, err := GetTableNames(db, database)
	if err != nil {
		return "", err
	}

	// 查询每个表的建表语句
	for _, tableName := range tables {
		createTableSQL, err := GetCreateTableSQL(db, database, tableName)
		if err != nil {
			return "", err
		}
		arr = append(arr, createTableSQL)
	}
	ddl = strings.Join(arr, ";\n")
	ddl = fmt.Sprintf("%s;\n", ddl) // 最后增加;表示结束
	err = SetDDLCache(cacheFilename, ddl)
	if err != nil { // 设置缓存
		return "", err
	}
	return ddl, nil
}

// 获取数据库中所有表名
func GetTableNames(db *sql.DB, dataBase string) (tables []string, err error) {
	defer func() {
		err = errors.WithMessagef(err, "database:%s", dataBase)
	}()
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables = make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// 获取指定表的建表语句
func GetCreateTableSQL(db *sql.DB, database string, tableName string) (ddl string, err error) {
	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, tableName)
	defer func() {
		err = errors.WithMessagef(err, "sql:%s", sql)
	}()
	dbx := sqlx.NewDb(db, "mysql") // 此处db.QueryRow 有时候返回2个，有时候4个，所有转sqlx查询
	row := dbx.QueryRowx(sql)
	if row.Err() != nil {
		return "", row.Err()
	}
	record, err := row.SliceScan() // 列数量和名称，不同db实例可能不一样，但是第一个库名，第二个创建语句
	if err != nil {
		return "", err
	}
	if len(record) < 2 {
		err = errors.Errorf("excepted at least2 col,got:%v", record)
		return "", err
	}
	ddl = cast.ToString(record[1])
	if !strings.Contains(ddl, "CREATE") && !strings.Contains(ddl, "create") {
		err = errors.Errorf("excepted ddl,got:%v", ddl)
		return "", err
	}

	return ddl, nil
}
