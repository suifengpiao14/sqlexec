package sqlexecparser

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	executor "github.com/suifengpiao14/ddl-executor"
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
	sort.Sort(tables)
	return tables, nil

}

// splitDDLStatements 使用逐个字符读取方式分割 DDL 语句（排除在引号内的分号）
func splitDDLStatements(batchDDL string) []string {
	var statements []string
	var currentStatement strings.Builder
	var insideSingleQuote, insideDoubleQuote bool
	batchDDL = fmt.Sprintf("%s;", batchDDL) // 最后增加; 确保 最后一个currentStatement 数据也收集了（因为后面有去除空白语句，所以多个;不影响结果）
	for _, char := range batchDDL {
		currentStatement.WriteRune(char)

		switch char {
		case ';':
			if !insideSingleQuote && !insideDoubleQuote {
				s := currentStatement.String()
				statements = append(statements, s)
				currentStatement.Reset()
			}
		case '\'':
			insideSingleQuote = !insideSingleQuote
		case '"':
			insideDoubleQuote = !insideDoubleQuote
		}
	}

	// 去除空白语句
	var nonEmptyStatements []string
	for _, statement := range statements {
		trimmed := strings.TrimSpace(statement)
		if trimmed != "" {
			nonEmptyStatements = append(nonEmptyStatements, trimmed)
		}
	}

	return nonEmptyStatements
}

const (
	// 需要加``,否则关键词作为库名、表名、列明会报错 如 replace
	Create_DB_SQL_Format = "create database `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
	Use_DB_SQL_Format    = "use `%s`;"
)

// TryExecDDLs 尝试解析ddls,其中,包含数据库不存在情况,自动创建
func TryExecDDLs(ddls string) (db *executor.Executor, err error) {
	//ddls = RemoveComments(ddls) // 'xxx#xx' 单引号中的# 有问题，这个地方感觉无需要去除注释，暂时注释，后续需要再完善
	conf := executor.NewDefaultConfig()
	db = executor.NewExecutor(conf)
	ddls = strings.Join(strings.Fields(ddls), " ")
	sqls := splitDDLStatements(ddls)
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
			err = errors.WithMessagef(err, "db:%s,ddl:%s", db.GetCurrentDatabase(), sql)
			return nil, err
		}
		switch executorErr.Code() {
		case executor.ErrBadDB.Code():
			var dbName string
			dbName, err = getDatabaseNameFromError(*executorErr, ERROR_UNKNOW_DATABASE_SCAN_FORMAT) //此处error 必须使用外部err
			if err != nil {
				return nil, err
			}
			if dbName != "" {
				arr := []string{
					fmt.Sprintf(Create_DB_SQL_Format, dbName),
					fmt.Sprintf(Use_DB_SQL_Format, dbName),
					sql,
				}
				sql = strings.Join(arr, "")
				err = db.Exec(sql)
				if err != nil {
					err = errors.WithMessagef(err, "db:%s,ddl:%s", db.GetCurrentDatabase(), sql)
					return nil, err
				}
			}
		case executor.ErrNoDB.Code():
			databases := db.GetDatabases()
			for _, dbName := range databases { // 默认使用第一个数据库
				sql = strings.Join([]string{
					fmt.Sprintf(Use_DB_SQL_Format, dbName),
					sql,
				}, "")
				err = db.Exec(sql) // 重新设置error
				if err != nil {
					err = errors.WithMessagef(err, "db:%s,ddl:%s", db.GetCurrentDatabase(), sql)
					return nil, err
				}
				break
			}
		}
		if err != nil { // err 处理不了，直接返回
			err = errors.WithMessagef(err, "db:%s,ddl:%s", db.GetCurrentDatabase(), sql)
			return nil, err
		}
	}
	return db, nil
}

const (
	ERROR_UNKNOW_DATABASE_SCAN_FORMAT = "Error 1049: Unknown database %s"
)

func getDatabaseNameFromError(executorErr executor.Error, format string) (dbName string, err error) {
	_, err = fmt.Sscanf(executorErr.Error(), format, &dbName)
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
var TypeForMysqlToGo = map[string]string{
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

func Mysql2GoType(mysqlType string, time2str bool) (goType string, size int, err error) {
	if time2str {
		TypeForMysqlToGo["date"] = "string"
		TypeForMysqlToGo["datetime"] = "string"
		TypeForMysqlToGo["timestamp"] = "string"
		TypeForMysqlToGo["time"] = "string"
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
	goType, ok := TypeForMysqlToGo[subType]
	if !ok {
		err = errors.Errorf("mysql2GoType: not found mysql type %s to go type", mysqlType)
	}
	return

}

func ConvertTabDef2Table(tableDef executor.TableDef) (table *Table, err error) {
	table = &Table{
		DBName:      DBName(tableDef.Database),
		TableName:   TableName(tableDef.Name),
		Columns:     make(Columns, 0),
		Comment:     tableDef.Comment,
		Constraints: make(Constraints, 0),
	}
	for _, indice := range tableDef.Indices {
		switch indice.Key {
		case executor.IndexType_PRI:
			table.Constraints.Add(Constraint_Type_Primary, ToColumnName(indice.Columns...)...)
		case executor.IndexType_UNI:
			table.Constraints.Add(Constraint_Type_Uniqueue, ToColumnName(indice.Columns...)...)
		}
	}
	for _, columnDef := range tableDef.Columns {
		goType, size, err := Mysql2GoType(columnDef.Type, true)

		if err != nil {
			return nil, err
		}
		column := Column{
			DBName:        table.DBName,
			TableName:     table.TableName,
			ColumnName:    ColumnName(columnDef.Name),
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

		column.PrimaryKey = column.PrimaryKey || table.Constraints.IsPrimaryKeyPart(column.ColumnName) // 补充主键
		column.UniqKey = column.UniqKey || table.Constraints.IsUniqKeyPart(column.ColumnName)          // 补充唯一键

		table.Columns = append(table.Columns, column)
	}
	return
}
