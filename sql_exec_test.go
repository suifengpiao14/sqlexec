package sqlexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
