package parser

import (
	"encoding/json"

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
	Column   string //列
	Value    string //值
	Operator string // 操作
}

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
		if columnValue.Column == column && columnValue.Operator == operator {
			return &columnValue, true
		}
	}
	return nil, false
}

type SQLTpl struct {
	Comments []string     `json:"comments"`
	Example  string       `json:"example"`
	Update   ColumnValues `json:"update"`
	Where    ColumnValues `json:"where"`
	Insert   ColumnValues `json:"insert"`
}

func (sqlTpl SQLTpl) String() string {
	b, err := json.Marshal(sqlTpl)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s
}

func ParseSQL(sqlStr string) (sqlTpl *SQLTpl, err error) {
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return nil, err
	}
	sqlTpl = &SQLTpl{
		Comments: make([]string, 0),
		Update:   make(ColumnValues, 0),
		Where:    make(ColumnValues, 0),
		Insert:   make(ColumnValues, 0),
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
		}
		sqlTpl.Example = sqlparser.String(stmt)

	case *sqlparser.Insert:
		for _, column := range stmt.Columns {
			sqlTpl.Insert.AddIgnore(ColumnValue{
				Column: column.String(),
			})
		}
		sqlTpl.Example = sqlparser.String(stmt)

	case *sqlparser.Select:
		whereColumnValues := ParseWhere(stmt.Where)
		sqlTpl.Where.AddIgnore(whereColumnValues...)
		for _, comment := range stmt.Comments {
			sqlTpl.Comments = append(sqlTpl.Comments, string(comment))
		}
	}
	sqlTpl.Example = sqlparser.String(stmt)
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
