package sqlexecparser_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func TestTryExecDDLs(t *testing.T) {
	file := "./xyxz_manage_db.sql"
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	ddls := string(b)
	tables, err := sqlexecparser.TryExecDDLs(ddls)
	require.NoError(t, err)
	fmt.Println(tables)
}
