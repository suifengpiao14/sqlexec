package sqlstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/logchan/v2"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
)

type DBConfig struct {
	DSN         string `json:"dsn"`
	LogLevel    string `json:"logLevel"`
	Timeout     int    `json:"timeout"`
	MaxOpen     int    `json:"maxOpen"`
	MaxIdle     int    `json:"maxIdle"`
	MaxIdleTime int    `json:"maxIdleTime"`
}

type ExecutorSQL struct {
	dbConfig  DBConfig
	sshConfig *SSHConfig
	_db       *sql.DB
	once      sync.Once
}

func NewExecutorSQL(dbConfig DBConfig, sshConfig *SSHConfig) (e *ExecutorSQL) {
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
	err = Byte2Struct([]byte(str), out)
	if err != nil {
		return err
	}
	return nil
}

var DriverName = "mysql"

func connectDB(cfg DBConfig, sshConfig *SSHConfig) (db *sql.DB, err error) {
	if sshConfig != nil {
		db, err = sshConfig.Tunnel(cfg.DSN)
	} else {
		db, err = sql.Open(DriverName, cfg.DSN)
	}
	return db, err
}

func Byte2Struct(data []byte, dst any) (err error) {
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

func ExecOrQueryContext(ctx context.Context, sqlDB *sql.DB, sqls string) (out string, err error) {
	sqlLogInfo := &LogInfoEXECSQL{}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()
	sqls = funcs.StandardizeSpaces(funcs.TrimSpaces(sqls)) // 格式化sql语句
	stmt, err := sqlparser.Parse(sqls)
	if err != nil {
		return "", errors.WithMessage(err, sqls)
	}
	sqlLogInfo.SQL = sqls
	switch stmt.(type) {
	case *sqlparser.Select:
		return QueryContext(ctx, sqlDB, sqls)
	case *sqlparser.Update:
		_, rowsAffected, err := ExecContext(ctx, sqlDB, sqls)
		if err != nil {
			return "", err
		}
		return cast.ToString(rowsAffected), nil
	case *sqlparser.Insert:
		lastInsertId, rowsAffected, err := ExecContext(ctx, sqlDB, sqls)
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
		_, rowsAffected, err := ExecContext(ctx, sqlDB, sqls)
		if err != nil {
			return "", err
		}
		return cast.ToString(rowsAffected), nil
	}

	return out, nil
}

func ExecContext(ctx context.Context, sqlDB *sql.DB, sqls string) (lastInsertId int64, rowsAffected int64, err error) {
	sqlLogInfo := &LogInfoEXECSQL{
		SQL: sqls,
	}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()

	sqlLogInfo.BeginAt = time.Now().Local()
	res, err := sqlDB.ExecContext(ctx, sqls)
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

func QueryContext(ctx context.Context, sqlDB *sql.DB, sqls string) (out string, err error) {
	sqlLogInfo := &LogInfoEXECSQL{
		SQL: sqls,
	}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()

	v, err, _ := execOrQueryContextSingleflight.Do(sqls, func() (interface{}, error) {
		sqlLogInfo.BeginAt = time.Now().Local()
		rows, err := sqlDB.QueryContext(ctx, sqls)
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
