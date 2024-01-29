package sqlexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec"
	"github.com/suifengpiao14/sshmysql"
)

func GetExecutorSQL() (executorSql *sqlexec.ExecutorSQL) {
	dbConfig := sqlexec.DBConfig{
		DSN: `root:1b03f8b486908bbe34ca2f4a4b91bd1c@mysql(127.0.0.1:3306)/curdservice?charset=utf8&timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`,
	}
	sshConfig := &sshmysql.SSHConfig{
		Address:        "120.24.156.100:2221",
		User:           "root",
		PriviteKeyFile: "C:\\Users\\Admin\\.ssh\\id_rsa",
	}

	executorSql = sqlexec.NewExecutorSQL(dbConfig, sshConfig)
	return executorSql

}

func TestSelect(t *testing.T) {
	ctx := context.Background()
	t.Run("select", func(t *testing.T) {
		sql := "select * from service where 1=1;"
		var out interface{}
		executorSql := GetExecutorSQL()
		err := executorSql.ExecOrQueryContext(ctx, sql, &out)
		require.NoError(t, err)
		fmt.Println(out)
	})
	t.Run("count", func(t *testing.T) {
		sql := "select count(*) from service where 1=1;"
		var out interface{}
		executorSql := GetExecutorSQL()
		err := executorSql.ExecOrQueryContext(ctx, sql, &out)
		require.NoError(t, err)
		fmt.Println(out)
	})
	t.Run("update", func(t *testing.T) {
		sql := "update service set name='test3' where id=1;"
		var out interface{}
		executorSql := GetExecutorSQL()
		err := executorSql.ExecOrQueryContext(ctx, sql, &out)
		require.NoError(t, err)
		fmt.Println(out)
	})
	t.Run("insert", func(t *testing.T) {
		sql := "insert into service (name) values('a2'),('a3');"
		var out interface{}
		executorSql := GetExecutorSQL()
		err := executorSql.ExecOrQueryContext(ctx, sql, &out)
		require.NoError(t, err)
		fmt.Println(out)
	})
}
