package sqlexecparser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type SQLTree struct {
	Comment    Comment `json:"comment"`
	ColumnName string  `json:"columnName"`
	Expr       string  `json:"expr"`
	Type       string  `json:"type"`
}

const (
	SQLTree_type_Update = "set"
	SQLTree_type_where  = "where"
	SQLTree_type_select = "select"
)

type Comment struct {
	Text       string `json:"text"`
	Required   bool   `json:"required,string"`
	ColumnName string `json:"columnName"`
}

type ColumnValue struct {
	Column   string `json:"column"`   //列
	Value    string `json:"value"`    //值
	Operator string `json:"operator"` // 操作
}

/*
func (c ColumnValue) MarshalJSON() ([]byte, error) {
	var w bytes.Buffer
	w.WriteByte('{')
	w.WriteString(fmt.Sprintf(`"column":"%s",`, c.Column))
	w.WriteString(fmt.Sprintf(`"value":"%s",`, c.Value))
	w.WriteString(fmt.Sprintf(`"operator":"%s"`, c.Operator))
	w.WriteByte('}')
	s := w.String()
	return []byte(s), nil
}
*/

type ColumnValues []ColumnValue

func (cvs *ColumnValues) Array() (columns []string, values []string) {
	columns = make([]string, 0)
	values = make([]string, 0)
	for _, cv := range *cvs {
		columns = append(columns, cv.Column)
		values = append(values, cv.Value)
	}
	return columns, values
}
func (cvs *ColumnValues) AddIgnore(columnValues ...ColumnValue) {
	for _, columnValue := range columnValues {
		_, ok := cvs.GetByColumn(columnValue.Column, columnValue.Operator)
		if ok {
			continue
		}
		*cvs = append(*cvs, columnValue)
	}
}

func (c ColumnValues) GetByColumn(column string, operator string) (col *ColumnValue, ok bool) {
	for _, columnValue := range c {
		if columnValue.Column == column && strings.EqualFold(columnValue.Operator, operator) {
			return &columnValue, true
		}
	}
	return nil, false
}

type SQLTpl struct {
	Comments    []string      `json:"comments"`
	Tpl         string        `json:"tpl"`
	Example     string        `json:"example"`
	Update      ColumnValues  `json:"update"`
	Where       ColumnValues  `json:"where"`
	Insert      ColumnValues  `json:"insert"`
	PlaceHodler []PlaceHodler `json:"placeHodler"`
	Metas       []Meta        `json:"metas"`
}

// Meta 记录列的属性,解析注释产生
type Meta struct {
	Column     string   `json:"column"`
	Attributes []string `json:"attributes"` // 记录列属性,比如必填存在 required
}

func (m *Meta) AddAttribute(attribute string) {
	if m.Attributes == nil {
		m.Attributes = make([]string, 0)
	}
	// 排重
	for _, attr := range m.Attributes {
		if strings.EqualFold(attr, attribute) {
			return
		}
	}
	m.Attributes = append(m.Attributes, attribute)
}

type Metas []Meta

func (ms *Metas) AddIgnore(metas ...Meta) {
	for _, m := range metas {
		exists := false
		for _, em := range *ms {
			if strings.EqualFold(em.Column, m.Column) {
				exists = true
			}
		}
		if exists {
			continue
		}
		*ms = append(*ms, m)
	}
}

type PlaceHodler struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type PlaceHodlers []PlaceHodler

func (ps PlaceHodlers) GetByType(typ string) (placeHodler *PlaceHodler, ok bool) {
	for _, p := range ps {
		if strings.EqualFold(p.Type, typ) {
			return &p, true
		}
	}
	return nil, true
}

const (
	PlaceHolder_Type_Where = "wehre"
	PlaceHolder_Type_Set   = "set"
	PlaceHolder_Type_Value = "value"
)

var DefaultPlaceHodler = PlaceHodlers{
	{
		Type: PlaceHolder_Type_Where,
		Text: fmt.Sprintf(`%s = '%s'`, PlaceHolder_Where_Column, PlaceHolder_Where_Value), //whereColumn = 'whereValue'
	},
	{
		Type: PlaceHolder_Type_Set,
		Text: fmt.Sprintf(`%s = '%s'`, PlaceHolder_Set_Column, PlaceHolder_Set_Value), //setColumn = 'setValue'
	},
	{
		Type: PlaceHolder_Type_Value,
		Text: fmt.Sprintf(`(%s) values ('%s')`, PlaceHolder_Values_Column, PlaceHolder_Values_Value), //(valueColumn) values ('valueValue')
	},
}

func (sqlTpl SQLTpl) String() string {
	sqlTpl.PlaceHodler = DefaultPlaceHodler // 携带占位符数据,方便调用方替换
	var w bytes.Buffer
	encoder := json.NewEncoder(&w)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(sqlTpl)
	if err != nil {
		panic(err)
	}
	s := w.String()
	return s
}

const (
	PlaceHolder_Set_Column   = "setColumn"
	PlaceHolder_Set_Value    = "setValue"
	PlaceHolder_Where_Column = "whereColumn"
	PlaceHolder_Where_Value  = "whereValue"

	PlaceHolder_Values_Column = "valueColumn"
	PlaceHolder_Values_Value  = "valueValue"
)

func ParseSQL(sqlStr string) (sqlTpl *SQLTpl, err error) {
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return nil, err
	}
	comments := extractComments(sqlStr)
	metas := parseComments(comments...)

	sqlTpl = &SQLTpl{
		Comments:    comments,
		Update:      make(ColumnValues, 0),
		Where:       make(ColumnValues, 0),
		Insert:      make(ColumnValues, 0),
		Example:     sqlparser.String(stmt),
		PlaceHodler: DefaultPlaceHodler,
		Metas:       metas,
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		for _, expr := range stmt.Exprs {
			colName := expr.Name.Name.String()
			colValue := sqlparser.String(expr.Expr)
			sqlTpl.Update.AddIgnore(ColumnValue{
				Column:   colName,
				Value:    colValue,
				Operator: "=",
			})
		}

		if stmt.Where != nil {
			whereColumnValues := ParseWhere(stmt.Where)
			sqlTpl.Where.AddIgnore(whereColumnValues...)
			// 构建where占位符
			valExpr := &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(PlaceHolder_Where_Value)}
			colIdent := sqlparser.NewColIdent(PlaceHolder_Where_Column)
			colName := &sqlparser.ColName{Name: colIdent}
			whereExpr := &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     colName,
				Right:    valExpr,
			}
			stmt.Where = sqlparser.NewWhere(sqlparser.WhereStr, whereExpr)
		}
		// 构建set占位符
		colIdent := sqlparser.NewColIdent(PlaceHolder_Set_Column)
		column := &sqlparser.ColName{Name: colIdent}
		updateExpr := &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(PlaceHolder_Set_Value)}
		assignment := &sqlparser.UpdateExpr{Name: column, Expr: updateExpr}
		stmt.Exprs = sqlparser.UpdateExprs{
			assignment,
		}

		sqlTpl.Tpl = sqlparser.String(stmt)

	case *sqlparser.Insert:
		for _, column := range stmt.Columns {
			sqlTpl.Insert.AddIgnore(ColumnValue{
				Column: column.String(),
			})
		}

		colIdent := sqlparser.NewColIdent(PlaceHolder_Values_Column)
		stmt.Columns = sqlparser.Columns{
			colIdent,
		}
		valExpr := &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(PlaceHolder_Values_Value)}
		stmt.Rows = sqlparser.Values{
			{valExpr},
		}
		sqlTpl.Tpl = sqlparser.String(stmt)

	case *sqlparser.Select:
		whereColumnValues := ParseWhere(stmt.Where)
		sqlTpl.Where.AddIgnore(whereColumnValues...)
		// 构建where占位符
		valExpr := &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(PlaceHolder_Where_Value)}
		colIdent := sqlparser.NewColIdent(PlaceHolder_Where_Column)
		colName := &sqlparser.ColName{Name: colIdent}
		whereExpr := &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualStr,
			Left:     colName,
			Right:    valExpr,
		}
		stmt.Where = sqlparser.NewWhere(sqlparser.WhereStr, whereExpr)
		sqlTpl.Tpl = sqlparser.String(stmt)
	}
	return sqlTpl, nil
}

func ParseWhere(whereExpr *sqlparser.Where) (columnValues ColumnValues) {
	columnValues = make(ColumnValues, 0)
	whereExpr.WalkSubtree(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch expr := node.(type) {
		case *sqlparser.ComparisonExpr:
			whereCol := sqlparser.String(expr.Left)
			whereVal := sqlparser.String(expr.Right)
			columnValues.AddIgnore(ColumnValue{
				Column:   whereCol,
				Value:    whereVal,
				Operator: expr.Operator,
			})
		}
		return true, nil
	})
	return columnValues
}

// extractComments 去除SQL中的注释
func extractComments(sql string) []string {
	// ("(""|[^"]|(\"))*") 双引号中的内容, "", "\""
	// ('(''|[^']|(\'))*') 单引号中的内容, '', '\''
	// (--[^\n\r]*) 双减号注释
	// (#.*) 井号注释
	// (/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/) 多行注释
	commentRegex := regexp.MustCompile(`("(""|[^"]|(\"))*")|('(''|[^']|(\'))*')|(--[^\n\r]*)|(#.*)|(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)`)
	comments := make([]string, 0)
	res := commentRegex.FindAllString(sql, -1)
	for _, comment := range res {
		if (comment[0] == '"' && comment[len(comment)-1] == '"') ||
			(comment[0] == '\'' && comment[len(comment)-1] == '\'') ||
			(string(comment[:3]) == "/*!") {
			continue
		}

		comment = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(comment, "/*"), "*/"))
		arr := strings.Split(comment, "\n")
		for _, line := range arr {
			line = strings.TrimSpace(strings.Trim(line, "*- "))
			if line == "" {
				continue
			}
			comments = append(comments, line)
		}
	}
	return comments
}

const (
	Meta_Attribute_Required = "required"
)

func parseComments(comments ...string) (metas Metas) {
	metas = make(Metas, 0)
	for _, comment := range comments {
		meta := Meta{}
		comment = strings.TrimSpace(comment)
		meta.Column = comment
		spaceIndex := strings.Index(comment, " ")
		if spaceIndex > -1 {
			meta.Column = comment[:spaceIndex]
		}
		//解析必填字断
		if strings.Contains(comment, Meta_Attribute_Required) {
			meta.AddAttribute(Meta_Attribute_Required)
		}
		if len(meta.Attributes) > 0 { //存在属性则收集
			metas.AddIgnore(meta)
		}
	}
	return metas
}