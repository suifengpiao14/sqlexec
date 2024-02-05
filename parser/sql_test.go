package parser_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/parser"
)

func TestParseSQL(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		sql := "update user set name='張三' where id=1;"
		sqlTpl, err := parser.ParseSQL(sql)
		require.NoError(t, err)
		setPlaceHodler, _ := parser.DefaultPlaceHodler.GetByType(parser.PlaceHolder_Type_Set)
		wherePlaceHodler, _ := parser.DefaultPlaceHodler.GetByType(parser.PlaceHolder_Type_Where)
		assert.Contains(t, sqlTpl.Tpl, setPlaceHodler.Text, wherePlaceHodler.Text)
		fmt.Println(sqlTpl.String())
	})
	t.Run("insert", func(t *testing.T) {
		sql := "insert user (id,name) values(1,'張三')"
		sqlTpl, err := parser.ParseSQL(sql)
		require.NoError(t, err)
		insertPlaceHodler, _ := parser.DefaultPlaceHodler.GetByType(parser.PlaceHolder_Type_Value)
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
		sqlTpl, err := parser.ParseSQL(sql)
		require.NoError(t, err)
		wherePlaceHodler, _ := parser.DefaultPlaceHodler.GetByType(parser.PlaceHolder_Type_Where)
		assert.Contains(t, sqlTpl.String(), wherePlaceHodler.Text)
		fmt.Println(sqlTpl.String())
	})

}
