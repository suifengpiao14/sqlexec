package sqlstream_test

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
}
