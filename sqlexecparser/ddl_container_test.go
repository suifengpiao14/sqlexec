package sqlexecparser_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func TestRegisterTableByDDL(t *testing.T) {
	database := "ad"
	err := sqlexecparser.RegisterTableByDDL(database, createDDLStr)
	require.NoError(t, err)
}
