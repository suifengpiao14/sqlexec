package sqlexecparser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	executor "github.com/bytewatch/ddl-executor"
	"github.com/pkg/errors"
)

// ParseDDL 解析sql ddl
func ParseDDL(ddls string) (tables Tables, err error) {
	tables = make(Tables, 0)
	db, err := TryExecDDLs(ddls)
	if err != nil {
		return
	}
	databases := db.GetDatabases()
	for _, dbName := range databases {
		tableNameList, err := db.GetTables(dbName)
		if err != nil {
			return nil, err
		}
		for _, tableName := range tableNameList {
			tableDef, err := db.GetTableDef(dbName, tableName)
			if err != nil {
				return nil, err
			}

			table, err := ConvertTabDef2Table(*tableDef)
			if err != nil {
				return nil, err
			}
			tables = append(tables, *table)
		}
	}
	return tables, nil

}

// TryExecDDLs 尝试解析ddls,其中,包含数据库不存在情况,自动创建
func TryExecDDLs(ddls string) (db *executor.Executor, err error) {
	ddls = RemoveComments(ddls)
	conf := executor.NewDefaultConfig()
	db = executor.NewExecutor(conf)
	ddls = strings.Join(strings.Fields(ddls), " ")
	sqls := strings.Split(ddls, ";")
	for _, sql := range sqls {
		if sql == "" {
			continue
		}
		err = db.Exec(sql)
		if err == nil {
			continue
		}
		executorErr, ok := err.(*executor.Error)
		if !ok {
			return nil, err
		}
		switch executorErr.Code() {
		case executor.ErrBadDB.Code():
			dbName, err := getDatabaseNameFromError(*executorErr, ERROR_UNKNOW_DATABASE_SCAN_FORMAT)
			if err != nil {
				return nil, err
			}
			if dbName != "" {
				sql = fmt.Sprintf("create database `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; use %s;%s", dbName, dbName, sql)
				err = db.Exec(sql)
				if err == nil {
					continue
				}
			}
		case executor.ErrNoDB.Code():
			databases := db.GetDatabases()
			if len(databases) == 1 {
				sql = fmt.Sprintf("use %s;%s", databases[0], sql)
				err = db.Exec(sql) // 重新设置error
				if err == nil {
					continue
				}
			}
		}
		if err != nil { // err 处理不了，直接返回
			return nil, err
		}
	}

	if err != nil {
		return
	}
	return
}

const (
	ERROR_UNKNOW_DATABASE_SCAN_FORMAT = "Error 1049: Unknown database %s"
)

func getDatabaseNameFromError(executorErr executor.Error, format string) (dbName string, err error) {
	_, err = fmt.Sscanf(executorErr.Error(), ERROR_UNKNOW_DATABASE_SCAN_FORMAT, &dbName)
	if err != nil {
		return "", err
	}
	dbName = strings.Trim(dbName, "'")
	return dbName, nil
}

func RemoveComments(query string) string {
	// 去除注释（包括单行和多行）
	//re := regexp.MustCompile(`/\*.*?\*/|(--.*$|\s*--.*$)`)
	re := regexp.MustCompile(`(--[^\n\r]*)|(#.*)|(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)`)
	return re.ReplaceAllString(query, "")
}

// map for converting mysql type to golang types
var typeForMysqlToGo = map[string]string{
	"int":                "int",
	"integer":            "int",
	"tinyint":            "int",
	"smallint":           "int",
	"mediumint":          "int",
	"bigint":             "int",
	"int unsigned":       "int",
	"integer unsigned":   "int",
	"tinyint unsigned":   "int",
	"smallint unsigned":  "int",
	"mediumint unsigned": "int",
	"bigint unsigned":    "int",
	"bit":                "int",
	"bool":               "bool",
	"enum":               "string",
	"set":                "string",
	"varchar":            "string",
	"char":               "string",
	"tinytext":           "string",
	"mediumtext":         "string",
	"text":               "string",
	"longtext":           "string",
	"blob":               "string",
	"tinyblob":           "string",
	"mediumblob":         "string",
	"longblob":           "string",
	"date":               "time.Time", // time.Time or string
	"datetime":           "time.Time", // time.Time or string
	"timestamp":          "time.Time", // time.Time or string
	"time":               "time.Time", // time.Time or string
	"float":              "float64",
	"double":             "float64",
	"decimal":            "float64",
	"binary":             "string",
	"varbinary":          "string",
}

func mysql2GoType(mysqlType string, time2str bool) (goType string, size int, err error) {
	if time2str {
		typeForMysqlToGo["date"] = "string"
		typeForMysqlToGo["datetime"] = "string"
		typeForMysqlToGo["timestamp"] = "string"
		typeForMysqlToGo["time"] = "string"
	}
	subType := mysqlType
	index := strings.Index(mysqlType, "(")
	if index > -1 {
		endIndex := strings.Index(mysqlType, ")")
		if endIndex > -1 { //获取大小
			number := mysqlType[index+1 : endIndex]
			size, _ = strconv.Atoi(number)
		}
		subType = mysqlType[:index]

	}
	goType, ok := typeForMysqlToGo[subType]
	if !ok {
		err = errors.Errorf("mysql2GoType: not found mysql type %s to go type", mysqlType)
	}
	return

}

func ConvertTabDef2Table(tableDef executor.TableDef) (table *Table, err error) {
	table = &Table{
		DBName:      tableDef.Database,
		TableName:   tableDef.Name,
		Columns:     make(Columns, 0),
		Comment:     tableDef.Comment,
		Constraints: make(Constraints, 0),
	}
	for _, indice := range tableDef.Indices {
		switch indice.Key {
		case executor.IndexType_PRI:
			table.Constraints.Add(Constraint_Type_Primary, indice.Columns...)
		case executor.IndexType_UNI:
			table.Constraints.Add(Constraint_Type_Uniqueue, indice.Columns...)
		}
	}
	for _, columnDef := range tableDef.Columns {

		goType, size, err := mysql2GoType(columnDef.Type, true)

		if err != nil {
			return nil, err
		}
		column := Column{
			ColumnName:    columnDef.Name,
			DBType:        columnDef.Type,
			GoType:        goType,
			Comment:       columnDef.Comment,
			Size:          size,
			Nullable:      columnDef.Nullable,
			Enums:         columnDef.Elems,
			AutoIncrement: columnDef.AutoIncrement,
			PrimaryKey:    columnDef.PrimaryKey,
			UniqKey:       columnDef.UniqKey,
			DefaultValue:  columnDef.DefaultValue,
			OnUpdate:      columnDef.OnUpdate,
			Unsigned:      columnDef.Unsigned,
		}

		table.Columns = append(table.Columns, column)
	}
	return
}
