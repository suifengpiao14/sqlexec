package sqlexec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/logchan/v2"
	"github.com/suifengpiao14/sshmysql"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
	gormLogger "gorm.io/gorm/logger"
)

type DBConfig struct {
	DSN         string `json:"dsn"`
	LogLevel    string `json:"logLevel"`
	Timeout     int    `json:"timeout"`
	MaxOpen     int    `json:"maxOpen"`
	MaxIdle     int    `json:"maxIdle"`
	MaxIdleTime int    `json:"maxIdleTime"`
}

//JsonToDBConfig 内置将json字符串转为DBConfig
func JsonToDBConfig(s string) (c *DBConfig, err error) {
	c = &DBConfig{}
	err = json.Unmarshal([]byte(s), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type ExecutorSQL struct {
	dbConfig  DBConfig
	sshConfig *sshmysql.SSHConfig
	_db       *sql.DB
	once      sync.Once
}

func NewExecutorSQL(dbConfig DBConfig, sshConfig *sshmysql.SSHConfig) (e *ExecutorSQL) {
	return &ExecutorSQL{
		dbConfig:  dbConfig,
		sshConfig: sshConfig,
	}
}

func (e *ExecutorSQL) GetDB() (db *sql.DB) {
	e.once.Do(func() {
		cfg := e.dbConfig
		db, err := connectDB(cfg, e.sshConfig)
		if err != nil {
			if errors.Is(err, &net.OpError{}) {
				err = nil
				time.Sleep(100 * time.Millisecond)
				db, err = connectDB(cfg, e.sshConfig)
			}
		}
		if err != nil {
			panic(err)
		}
		sqlDB := db
		sqlDB.SetMaxOpenConns(cfg.MaxOpen)
		sqlDB.SetMaxIdleConns(cfg.MaxIdle)
		sqlDB.SetConnMaxIdleTime(time.Duration(cfg.MaxIdleTime) * time.Minute)
		e._db = db
	})
	return e._db
}

func (e *ExecutorSQL) ExecOrQueryContext(ctx context.Context, sqls string, out interface{}) (err error) {
	str, err := ExecOrQueryContext(ctx, e.GetDB(), sqls)
	if err != nil {
		return err
	}
	err = byte2Struct([]byte(str), out)
	if err != nil {
		return err
	}
	return nil
}

var DriverName = "mysql"

func connectDB(cfg DBConfig, sshConfig *sshmysql.SSHConfig) (db *sql.DB, err error) {
	if sshConfig != nil {
		db, err = sshConfig.Tunnel(cfg.DSN)
	} else {
		db, err = sql.Open(DriverName, cfg.DSN)
	}
	return db, err
}

func byte2Struct(data []byte, dst any) (err error) {
	if dst == nil {
		return
	}
	if len(data) == 0 {
		return nil
	}
	rv := reflect.Indirect(reflect.ValueOf(dst))
	rt := rv.Type()
	str := string(data)
	switch rt.Kind() {
	case reflect.Map, reflect.Struct:
		result := gjson.Parse(str)
		if result.IsArray() {
			str = result.Get("@this.0").String()
		}
	}
	b := []byte(str)
	err = json.Unmarshal(b, dst)
	if err != nil {
		return err
	}
	return nil
}

var execOrQueryContextSingleflight = new(singleflight.Group)

func ExecOrQueryContext(ctx context.Context, db *sql.DB, sqls string) (out string, err error) {
	sqlLogInfo := &LogInfoEXECSQL{}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()
	//sqls = funcs.StandardizeSpaces(funcs.TrimSpaces(sqls)) // 格式化sql语句 // 语句中间的\n \t 等保持，比如保存http协议，就必须保存\n,如果get请求，只有header，没有body，最后的\r\n 也必须保留，所以注释这个地方
	stmt, err := sqlparser.Parse(sqls)
	if err != nil {
		return "", errors.WithMessage(err, sqls)
	}
	sqlLogInfo.SQL = sqls
	switch stmt.(type) {
	case *sqlparser.Select:
		return QueryContext(ctx, db, sqls)
	case *sqlparser.Update:
		_, rowsAffected, err := ExecContext(ctx, db, sqls)
		if err != nil {
			return "", err
		}
		return cast.ToString(rowsAffected), nil
	case *sqlparser.Insert:
		lastInsertId, rowsAffected, err := ExecContext(ctx, db, sqls)
		if err != nil {
			return "", err
		}
		insertIdArr := make([]string, 0)
		for i := int64(0); i < rowsAffected; i++ {
			insertIdArr = append(insertIdArr, cast.ToString(lastInsertId+i))
		}
		b, err := json.Marshal(insertIdArr)
		if err != nil {
			return "", err
		}
		out = string(b)
		return out, nil

	case *sqlparser.Delete:
		_, rowsAffected, err := ExecContext(ctx, db, sqls)
		if err != nil {
			return "", err
		}
		return cast.ToString(rowsAffected), nil
	}

	return out, nil
}

func ExecContext(ctx context.Context, db *sql.DB, sqls string) (lastInsertId int64, rowsAffected int64, err error) {
	sqlLogInfo := &LogInfoEXECSQL{
		SQL: sqls,
	}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()

	sqlLogInfo.BeginAt = time.Now().Local()
	res, err := db.ExecContext(ctx, sqls)
	if err != nil {
		return 0, 0, err
	}
	sqlLogInfo.EndAt = time.Now().Local()
	lastInsertId, _ = res.LastInsertId()
	rowsAffected, _ = res.RowsAffected()

	sqlLogInfo.RowsAffected = rowsAffected
	sqlLogInfo.LastInsertId = lastInsertId
	return lastInsertId, rowsAffected, nil

}

func QueryContext(ctx context.Context, db *sql.DB, sqls string) (out string, err error) {
	sqlLogInfo := &LogInfoEXECSQL{
		SQL: sqls,
	}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()

	v, err, _ := execOrQueryContextSingleflight.Do(sqls, func() (interface{}, error) {
		sqlLogInfo.BeginAt = time.Now().Local()
		rows, err := db.QueryContext(ctx, sqls)
		sqlLogInfo.EndAt = time.Now().Local()
		if err != nil {
			return out, err
		}
		defer func() {
			err := rows.Close()
			if err != nil {
				panic(err)
			}
		}()
		allResult := make([][]map[string]string, 0)
		rowsAffected := 0
		for {
			records := make([]map[string]string, 0)
			for rows.Next() {
				rowsAffected++
				var record = make(map[string]interface{})
				var recordStr = make(map[string]string)
				err := sqlx.MapScan(rows, record)
				if err != nil {
					return out, err
				}
				for k, v := range record {
					recordStr[k] = cast.ToString(v)
				}

				records = append(records, recordStr)
			}
			allResult = append(allResult, records)
			if !rows.NextResultSet() {
				break
			}
		}
		sqlLogInfo.RowsAffected = int64(rowsAffected)
		if len(allResult) == 1 { // allResult 初始值为[[]],至少有一个元素
			result := allResult[0]
			if len(result) == 0 { // 结果为空，返回空字符串
				return out, nil
			}
			if len(result) == 1 && len(result[0]) == 1 {
				row := result[0]
				for _, val := range row {
					return val, nil // 只有一个值时，直接返回值本身
				}
			}
			b, err := json.Marshal(result)
			if err != nil {
				return out, err
			}
			out = string(b)
			sqlLogInfo.Result = out
			return out, nil
		}

		jsonByte, err := json.Marshal(allResult)
		if err != nil {
			return out, err
		}
		out = string(jsonByte)
		sqlLogInfo.Result = out

		return out, nil
	})
	if err != nil {
		return out, err
	}
	out = v.(string)
	return out, nil
}

// ExplainSQL 将字named sql,数据整合为sql
func ExplainSQL(namedSql string, namedData map[string]any) (sql string, err error) {
	namedSql = strings.TrimSpace(namedSql)
	statment, arguments, err := sqlx.Named(namedSql, namedData)
	if err != nil {
		err = errors.WithStack(err)
		return "", err
	}
	sql = gormLogger.ExplainSQL(statment, nil, `'`, arguments...)
	return sql, nil
}

//ExplainNamedSQL 带占位符的sql模板绑定数据后转换为常规sql(可以替换ExplainSQL,相比ExplainSQL 能更好的支持in 条件查询) 调用前可以先使用MysqlRealEscapeString 转义字符
func ExplainNamedSQL(namedSQL string, namedData map[string]any) (string, error) {
	stmt, err := sqlparser.Parse(namedSQL)
	if err != nil {
		return "", err
	}
	bindVars := make(map[string]any)
	for key, val := range namedData {
		key = fmt.Sprintf(":%s", key)
		bindVars[key] = val
	}
	err = replacePlaceholdersRecursive(stmt, bindVars)
	if err != nil {
		return "", err
	}

	return sqlparser.String(stmt), nil
}

func replacePlaceholdersRecursive(node sqlparser.SQLNode, bindVars map[string]any) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.SQLVal:
			// Replace placeholders with bind variables
			if node.Type == sqlparser.ValArg {
				name := string(node.Val)
				val, ok := bindVars[name]
				if !ok {
					err = errors.Errorf("bind variable not found: %s", name)
					return false, err
				}
				node.Val, err = convertValue(val) // 放到这个地方转换，主要是只转换关心的数据，其它数据容许错误，提高容错能力
				if err != nil {
					return false, err
				}
			}
		}
		return true, nil
	}, node)
}

func convertValue(value any) ([]byte, error) {
	switch v := reflect.Indirect(reflect.ValueOf(value)); v.Kind() {
	case reflect.Int, reflect.Int64, reflect.Float64:
		return []byte(fmt.Sprintf("%v", value)), nil
	case reflect.String:
		return []byte(fmt.Sprintf("'%v'", v)), nil
	case reflect.Slice:
		// Handle []string and []int
		if v.Len() == 0 {
			err := errors.Errorf("empty slice")
			return nil, err
		}
		switch v.Index(0).Kind() {
		case reflect.String:
			strValues := make([]string, v.Len())
			for i := 0; i < v.Len(); i++ {
				strValues[i] = v.Index(i).String()
			}
			return []byte(fmt.Sprintf("'%s'", strings.Join(strValues, "','"))), nil
		case reflect.Int, reflect.Int64:
			intValues := make([]string, v.Len())
			for i := 0; i < v.Len(); i++ {
				intValues[i] = fmt.Sprintf("%d", v.Index(i).Int())
			}
			return []byte(strings.Join(intValues, ",")), nil
		case reflect.Float32, reflect.Float64:
			floatValues := make([]string, v.Len())
			for i := 0; i < v.Len(); i++ {
				floatValues[i] = fmt.Sprintf("%f", v.Index(i).Float())
			}
			return []byte(strings.Join(floatValues, ",")), nil
		default:
			return nil, fmt.Errorf("unsupported slice type: %s", v.Type())
		}

	default:
		return nil, fmt.Errorf("unsupported type: %s", v.Type())
	}
}

// MysqlRealEscapeString 初步的防sql注入
func MysqlRealEscapeString(value string) string {
	var sb strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\', 0, '\n', '\r', '\'', '"':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '\032':
			sb.WriteByte('\\')
			sb.WriteByte('Z')
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}
