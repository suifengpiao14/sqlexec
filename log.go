package sqlstream

import (
	"context"
	"fmt"
	"time"

	"github.com/suifengpiao14/logchan/v2"
)

type LogName string

func (l LogName) String() string {
	return string(l)
}

type LogInfoEXECSQL struct {
	Context      context.Context
	SQL          string    `json:"sql"`
	Result       []byte    `json:"result"`
	Err          error     `json:"error"`
	BeginAt      time.Time `json:"beginAt"`
	EndAt        time.Time `json:"endAt"`
	Duration     string    `json:"time"`
	RowsAffected int64     `json:"affectedRows"`
	LastInsertId int64     `json:"lastInsertId"`
	Level        string    `json:"level"`
	logchan.EmptyLogInfo
}

func (l *LogInfoEXECSQL) GetName() logchan.LogName {
	return LOG_INFO_EXEC_SQL
}
func (l *LogInfoEXECSQL) Error() error {
	return l.Err
}
func (l *LogInfoEXECSQL) GetLevel() string {
	return l.Level
}
func (l *LogInfoEXECSQL) BeforeSend() {
	duration := float64(l.EndAt.Sub(l.BeginAt).Nanoseconds()) / 1e6
	l.Duration = fmt.Sprintf("%.3fms", duration)
}

const (
	LOG_INFO_EXEC_SQL LogName = "LogInfoEXECSQL"
)

// DefaultPrintLogInfoEXECSQL 默认日志打印函数
func DefaultPrintLogInfoEXECSQL(logInfo logchan.LogInforInterface, typeName logchan.LogName, err error) {
	if typeName != LOG_INFO_EXEC_SQL {
		return
	}
	logInfoEXECSQL, ok := logInfo.(*LogInfoEXECSQL)
	if !ok {
		return
	}
	if err != nil {
		_, err1 := fmt.Fprintf(logchan.LogWriter, "%s|loginInfo:%s|error:%s\n", logchan.DefaultPrintLog(logInfoEXECSQL), logInfoEXECSQL.SQL, err.Error())
		if err1 != nil {
			fmt.Printf("err: DefaultPrintLogInfoEXECSQL fmt.Fprintf:%s\n", err1.Error())
		}
		return
	}
	_, err1 := fmt.Fprintf(logchan.LogWriter, "%s|SQL:%+s [%s rows:%d]\n", logchan.DefaultPrintLog(logInfoEXECSQL), logInfoEXECSQL.SQL, logInfoEXECSQL.Duration, logInfoEXECSQL.RowsAffected)
	if err1 != nil {
		fmt.Printf("err: DefaultPrintLogInfoEXECSQL fmt.Fprintf:%s\n", err1.Error())
	}
}
