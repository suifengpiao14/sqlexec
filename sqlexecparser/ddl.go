package sqlexecparser

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
	TableName   string      `json:"tableName"`
	Columns     Columns     `json:"columns"`
	Comment     string      `json:"comment"`
	Constraints Constraints `json:"constraints"`
}

var (
	ERROR_NOT_FOUND_PRIMARY_KEY = errors.New("not found primary key")
	ERROR_NOT_FOUND_COLUMN      = errors.New("not found column")
)

func (t Table) GetPrimaryKey() (columns Columns, err error) {
	c, ok := t.Constraints.GetByType(Constraint_Type_Primary)
	if !ok {
		err = errors.WithMessagef(ERROR_NOT_FOUND_PRIMARY_KEY, "table:%s", t.TableName)
		return nil, err

	}
	columns, err = t.Columns.GetByNames(c.ColumnNames...)
	if err != nil {
		return nil, err
	}
	return columns, nil
}

func (t Table) GetUniqKey() (columns Columns, err error) {
	c, ok := t.Constraints.GetByType(Constraint_Type_Uniqueue)
	if !ok {
		return nil, nil

	}
	columns, err = t.Columns.GetByNames(c.ColumnNames...)
	if err != nil {
		return nil, err
	}
	return columns, nil
}

type Constraints []Constraint

type Constraint struct {
	Type        string   `json:"type"`
	ColumnNames []string `json:"columnNames"`
}

const (
	Constraint_Type_Primary  = "primary"
	Constraint_Type_Uniqueue = "uniqueue"
)

func (c *Constraint) AddColumnName(columnNames ...string) {
	if c.ColumnNames == nil {
		c.ColumnNames = make([]string, 0)
	}
	for _, columnName := range columnNames {
		exists := false
		for _, cName := range c.ColumnNames {
			if strings.EqualFold(cName, columnName) {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		c.ColumnNames = append(c.ColumnNames, columnName)
	}

}

func (c Constraint) Equal(typ string, columnNames ...string) (yes bool) {
	if !strings.EqualFold(c.Type, typ) {
		return false
	}
	if len(columnNames) != len(c.ColumnNames) {
		return false
	}
	for _, name := range columnNames {
		exists := false
		for _, en := range c.ColumnNames {
			if strings.EqualFold(en, name) {
				exists = true
				break
			}
		}
		if !exists {
			return false
		}
	}
	return true
}

func (cs *Constraints) Add(typ string, columnNames ...string) {
	constraint := Constraint{
		Type: typ,
	}
	i := 0
	for i = 0; i < len(*cs); i++ {
		c := (*cs)[i]
		if strings.EqualFold(c.Type, typ) {
			constraint = c
			break
		}
	}
	constraint.AddColumnName(columnNames...)
	if i < len(*cs) {
		(*cs)[i] = constraint
		return
	}
	*cs = append(*cs, constraint)

}

func (cs Constraints) IsPrimaryKey(columnNames ...string) (yes bool) {
	primaryConstraint, ok := cs.GetByType(Constraint_Type_Primary)
	if !ok {
		return false
	}
	yes = primaryConstraint.Equal(Constraint_Type_Primary, columnNames...)
	return yes
}

func (cs Constraints) GetByType(typ string) (c *Constraint, ok bool) {
	for _, c := range cs {
		if strings.EqualFold(c.Type, typ) {
			return &c, true
		}
	}
	return nil, false
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

type Columns []Column

func (cs Columns) GetFirst() (first *Column, ok bool) {
	if len(cs) == 0 {
		return nil, false
	}
	return &cs[0], true
}
func (cs Columns) GetByName(name string) (column *Column, ok bool) {
	for _, c := range cs {
		if strings.EqualFold(name, c.ColumnName) {
			return &c, true
		}
	}
	return nil, false
}

func (cs Columns) GetByNames(names ...string) (columns Columns, err error) {
	columns = make(Columns, 0)
	for _, name := range names {
		col, ok := cs.GetByName(name)
		if !ok {
			err = errors.WithMessagef(ERROR_NOT_FOUND_PRIMARY_KEY, "column:%s", col.ColumnName)
			return nil, err
		}
		if ok {
			columns = append(columns, *col)
		}
	}
	return columns, nil
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

// ParseCreateDDL 解析建表ddl
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

// ParseOneCreateDDL 解析单个表
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
		TableName:   createTableStmt.NewName.Name.String(),
		Columns:     make(Columns, 0),
		Constraints: make(Constraints, 0),
	}
	for _, option := range createTableStmt.Options {
		if option.Type == sqlparser.TableOptionComment {
			table.Comment = option.StrValue
		}
	}

	for _, column := range createTableStmt.Columns {
		col := Column{
			ColumnName: column.Name,
			Type:       column.Type,
			Enums:      make([]string, 0), // 确保json化后为[],而不是null
		}
		col.Enums = append(col.Enums, column.Elems...)

		for _, option := range column.Options {
			switch option.Type {
			case sqlparser.ColumnOptionPrimaryKey:
				table.Constraints.Add(Constraint_Type_Primary, col.ColumnName)
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
		table.Columns = append(table.Columns, col)

		for _, constraint := range createTableStmt.Constraints {
			switch constraint.Type {
			case sqlparser.ConstraintPrimaryKey:
				for _, key := range constraint.Keys {
					table.Constraints.Add(Constraint_Type_Primary, key.String())
				}
			case sqlparser.ConstraintKey:
			case sqlparser.ConstraintIndex:
			case sqlparser.ConstraintUniq:
				for _, key := range constraint.Keys {
					table.Constraints.Add(Constraint_Type_Uniqueue, key.String())
				}
			case sqlparser.ConstraintUniqKey:
			case sqlparser.ConstraintUniqIndex:
			case sqlparser.ConstraintForeignKey:
			case sqlparser.ConstraintFulltext:

			}
		}
	}

	return table, err
}
