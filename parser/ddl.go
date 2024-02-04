package parser

import (
	"encoding/json"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

/*********************************本包仅仅解析ddl，已json格式返回表、列等信息,和具体使用场景的数据结合不包含在该包内**************************************/
/***********************************************************************/

type Table struct {
	TableName string    `json:"tableName"`
	Columns   []*Column `json:"columns"`
	Comment   string    `json:"comment"`
}

type Tables []Table

func (tbs Tables) String() string {
	b, err := json.Marshal(tbs)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s
}

type Column struct {
	PrimaryKey    bool     `json:"primaryKey,string"`
	ColumnName    string   `json:"columnName"`
	Type          string   `json:"type"`
	Comment       string   `json:"comment"`
	Nullable      bool     `json:"nullable,string"`
	Enums         []string `json:"enums"`
	AutoIncrement bool     `json:"autoIncrement,string"`
	UniqKey       bool     `json:"uniqKey,string"`
	DefaultValue  string   `json:"defaultValue"`
	OnUpdate      bool     `json:"onUpdate,string"`
}

func (t Table) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s
}

const (
	DEFAULT_VALUE_CURRENT_TIMESTAMP = "current_timestamp"
)

// IsDefaultValueCurrentTimestamp 判断默认值是否为自动填充时间
func (c *Column) IsDefaultValueCurrentTimestamp() bool {

	return strings.Contains(strings.ToLower(c.DefaultValue), DEFAULT_VALUE_CURRENT_TIMESTAMP) // 测试发现有 current_timestamp() 情况
}

//ParseCreateDDL 解析建表ddl
func ParseCreateDDL(ddlStatements string) (tables Tables, err error) {
	arr := strings.Split(ddlStatements, ";")
	tables = make(Tables, 0)
	for _, ddlStatement := range arr {
		ddlStatement = strings.TrimSpace(ddlStatement)
		if ddlStatement == "" {
			continue
		}
		table, err := ParseOneCreateDDL(ddlStatement)
		if err != nil {
			err = errors.WithMessagef(err, "ddl:%s", ddlStatement)
			return nil, err
		}
		tables = append(tables, *table)
	}
	return tables, nil
}

//ParseOneCreateDDL 解析单个表
func ParseOneCreateDDL(ddlStatement string) (table *Table, err error) {
	stmt, err := sqlparser.Parse(ddlStatement)
	if err != nil {
		return nil, err
	}

	// 处理 CREATE TABLE 语句
	createTableStmt, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		err = errors.Errorf("invalid CREATE TABLE statement")
		return nil, err
	}
	table = &Table{
		TableName: createTableStmt.NewName.Name.String(),
		Columns:   make([]*Column, 0),
	}
	for _, option := range createTableStmt.Options {
		if option.Type == sqlparser.TableOptionComment {
			table.Comment = option.StrValue
		}
	}

	for _, column := range createTableStmt.Columns {
		col := &Column{
			ColumnName: column.Name,
			Type:       column.Type,
			Enums:      make([]string, 0), // 确保json化后为[],而不是null
		}
		col.Enums = append(col.Enums, column.Elems...)
		table.Columns = append(table.Columns, col)
		for _, option := range column.Options {
			switch option.Type {
			case sqlparser.ColumnOptionPrimaryKey:
				col.PrimaryKey = cast.ToBool(option.Value)
			case sqlparser.ColumnOptionComment:
				col.Comment = strings.Trim(option.Value, `"'`)
			case sqlparser.ColumnOptionNotNull:
				col.Nullable = !cast.ToBool(option.Value)
			case sqlparser.ColumnOptionAutoIncrement:
				col.AutoIncrement = cast.ToBool(option.Value)
			case sqlparser.ColumnOptionDefaultValue:
				col.DefaultValue = option.Value
			case sqlparser.ColumnOptionUniqKey:
				col.UniqKey = cast.ToBool(option.Value)
			case sqlparser.ColumnOptionOnUpdate:
				col.OnUpdate = cast.ToBool(option.Value)
			}

		}
	}

	return table, err
}
