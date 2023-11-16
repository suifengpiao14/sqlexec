package sqlstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/cudevent/cudeventimpl"
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
	SSHConfig   *SSHConfig
}

type ExecutorSQL struct {
	config DBConfig
	_db    *sql.DB
	once   sync.Once
}

func NewExecutorSQL(dbConfig DBConfig) (e *ExecutorSQL) {
	return &ExecutorSQL{
		config: dbConfig,
	}
}

var DriverName = "mysql"

func connectDB(cfg DBConfig) (db *sql.DB, err error) {
	if cfg.SSHConfig != nil {
		db, err = cfg.SSHConfig.Tunnel(cfg.DSN)
	} else {
		db, err = sql.Open(DriverName, cfg.DSN)
	}
	return db, err
}

func (e *ExecutorSQL) GetDB() (db *sql.DB) {
	e.once.Do(func() {
		cfg := e.config
		db, err := connectDB(cfg)
		if err != nil {
			if errors.Is(err, &net.OpError{}) {
				err = nil
				time.Sleep(100 * time.Millisecond)
				db, err = connectDB(cfg)
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
	b, err := execOrQueryContext(ctx, e.GetDB(), sqls)
	if err != nil {
		return err
	}
	if b == nil {
		return
	}

	rt := reflect.Indirect(reflect.ValueOf(out)).Type()
	switch rt.Kind() {
	case reflect.Map, reflect.Struct:
		result := gjson.ParseBytes(b)
		if result.IsArray() {
			str := result.Get("@this.0").String()
			b = []byte(str)
		}
	}
	err = json.Unmarshal(b, out)
	if err != nil {
		return err
	}
	return nil

}

var execOrQueryContextSingleflight = new(singleflight.Group)

func execOrQueryContext(ctx context.Context, sqlDB *sql.DB, sqls string) (out []byte, err error) {
	sqlLogInfo := &LogInfoEXECSQL{}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()
	sqls = funcs.StandardizeSpaces(funcs.TrimSpaces(sqls)) // 格式化sql语句
	stmt, err := sqlparser.Parse(sqls)
	if err != nil {
		return nil, errors.WithMessage(err, sqls)
	}
	sqlLogInfo.SQL = sqls
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return QueryContext(ctx, sqlDB, sqls)
	case *sqlparser.Update:
		selectSQL := cudeventimpl.ConvertUpdateToSelect(stmt)
		before, err := QueryContext(ctx, sqlDB, selectSQL)
		if err != nil {
			return nil, err
		}
		sqlRawEvent := &cudeventimpl.SQLRawEvent{
			BeforeData: before,
		}
		_, rowsAffected, err := ExecContext(ctx, sqlDB, sqls, sqlRawEvent)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(rowsAffected, 10)), nil
	case *sqlparser.Insert:
		lastInsertId, _, err := ExecContext(ctx, sqlDB, sqls, nil)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(lastInsertId, 10)), nil

	case *sqlparser.Delete:
		_, rowsAffected, err := ExecContext(ctx, sqlDB, sqls, nil)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(rowsAffected, 10)), nil
	}

	return out, nil
}

func ExecContext(ctx context.Context, sqlDB *sql.DB, sqls string, sqlRawEvent *cudeventimpl.SQLRawEvent) (lastInsertId int64, rowsAffected int64, err error) {
	sqlLogInfo := &LogInfoEXECSQL{
		SQL: sqls,
	}
	defer func() {
		sqlLogInfo.Err = err
		logchan.SendLogInfo(sqlLogInfo)
	}()

	if sqlRawEvent == nil {
		sqlRawEvent = &cudeventimpl.SQLRawEvent{}
	}
	sqlRawEvent.DBExecutorGetter = func() (dbExecutor cudeventimpl.DBExecutor) {
		return &ExecutorSQL{
			_db: sqlDB,
		}
	}
	sqlRawEvent.SQL = sqls

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
	sqlRawEvent.RowsAffected = rowsAffected
	sqlRawEvent.LastInsertId = lastInsertId
	cudeventimpl.PublishSQLRawEventAsync(sqlRawEvent)
	return lastInsertId, rowsAffected, nil

}

func QueryContext(ctx context.Context, sqlDB *sql.DB, sqls string) (out []byte, err error) {
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
			return "", err
		}
		defer func() {
			err := rows.Close()
			if err != nil {
				panic(err)
			}
		}()
		allResult := make([][]map[string]string, 0)
		rowsAffected := 0
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return "", err
		}
		for _, columnType := range columnTypes {
			dbType := columnType.DatabaseTypeName()
			dbName := columnType.Name()
			_ = dbType
			_ = dbName
		}
		for {
			records := make([]map[string]string, 0)
			for rows.Next() {
				rowsAffected++
				var record = make(map[string]interface{})
				var recordStr = make(map[string]string)
				err := MapScan(rows, record)
				if err != nil {
					return "", err
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
				return "", nil
			}
			if len(result) == 1 && len(result[0]) == 1 {
				row := result[0]
				for _, val := range row {
					return val, nil // 只有一个值时，直接返回值本身
				}
			}
			out, err = json.Marshal(result)
			if err != nil {
				return "", err
			}
			sqlLogInfo.Result = out
			return out, nil
		}

		jsonByte, err := json.Marshal(allResult)
		if err != nil {
			return "", err
		}
		sqlLogInfo.Result = jsonByte

		return out, nil
	})
	if err != nil {
		return nil, err
	}
	out = v.([]byte)
	return out, nil
}

// MapScan copy sqlx
func MapScan(r *sql.Rows, dest map[string]interface{}) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	return r.Err()
}
