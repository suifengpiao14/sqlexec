package sqlexecparser_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/sqlexecparser"
)

func TestParseSQL(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		sql := `
		-- id required
		update user set name='張三' where id=1;`
		sqlTpl, err := sqlexecparser.ParseSQL(sql)
		require.NoError(t, err)
		setPlaceHodler, _ := sqlexecparser.DefaultPlaceHodler.GetByType(sqlexecparser.PlaceHolder_Type_Set)
		wherePlaceHodler, _ := sqlexecparser.DefaultPlaceHodler.GetByType(sqlexecparser.PlaceHolder_Type_Where)
		assert.Contains(t, sqlTpl.Tpl, setPlaceHodler.Text, wherePlaceHodler.Text)
		fmt.Println(sqlTpl.String())
	})
	t.Run("insert", func(t *testing.T) {
		sql := "insert user (id,name) values(1,'張三')"
		sqlTpl, err := sqlexecparser.ParseSQL(sql)
		require.NoError(t, err)
		insertPlaceHodler, _ := sqlexecparser.DefaultPlaceHodler.GetByType(sqlexecparser.PlaceHolder_Type_Value)
		assert.Contains(t, sqlTpl.String(), insertPlaceHodler.Text)
		fmt.Println(sqlTpl.String())
	})
	t.Run("select", func(t *testing.T) {
		//todo 注释解析
		sql := `
		/**
		id required
		**/
		-- id required
		select id,name,nickname from  user where id>1 and name like '%三';`
		sqlTpl, err := sqlexecparser.ParseSQL(sql)
		require.NoError(t, err)
		wherePlaceHodler, _ := sqlexecparser.DefaultPlaceHodler.GetByType(sqlexecparser.PlaceHolder_Type_Where)
		assert.Contains(t, sqlTpl.String(), wherePlaceHodler.Text)
		fmt.Println(sqlTpl.String())
	})

}
