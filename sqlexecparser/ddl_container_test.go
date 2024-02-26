package sqlexecparser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func TestRegisterTableByDDL(t *testing.T) {
	database := "ad"
	err := sqlexecparser.RegisterTableByDDL(database, createDDLStr)
	require.NoError(t, err)
}

func TestGetDBNameFromDSN(t *testing.T) {
	dsn := `xyxzapps:p3Ry5prmNHIxfd@tcp(hjx.m.mysql.hsb.com:3306)/xyxz_manage_db?charset=utf8&timeout=1s&readTimeout=5s&writeTimeout=5s&parseTime=False&loc=Local&multiStatements=true`
	dbname, err := sqlexecparser.GetDBNameFromDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, dbname, "xyxz_manage_db")
}
