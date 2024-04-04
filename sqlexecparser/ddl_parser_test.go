package sqlexecparser_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func TestTryExecDDLs(t *testing.T) {
	file := "./xyxz_manage_db.sql"
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	ddls := string(b)
	db, err := sqlexecparser.TryExecDDLs(ddls)
	require.NoError(t, err)
	tables, err := db.GetTables(db.GetCurrentDatabase())
	require.NoError(t, err)
	assert.Equal(t, 2, len(tables))
}
func TestTryExecDDLs2(t *testing.T) {
	file := "./video.sql"
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	ddls := string(b)
	db, err := sqlexecparser.TryExecDDLs(ddls)
	require.NoError(t, err)
	tables, err := db.GetTables(db.GetCurrentDatabase())
	require.NoError(t, err)
	assert.Equal(t, 5, len(tables))
}
