package sqlexecparser

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
)

const (
	DEFAULT_VALUE_CURRENT_TIMESTAMP = "current_timestamp"
)

var (
	ERROR_NOT_FOUND_PRIMARY_KEY = errors.New("not found primary key")
	ERROR_NOT_FOUND_COLUMN      = errors.New("not found column")
)

type Table struct {
	DBName      string      `json:"dbName"` // 额外增加
	TableName   string      `json:"tableName"`
	Columns     Columns     `json:"columns"`
	Comment     string      `json:"comment"`
	Constraints Constraints `json:"constraints"`
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

//根据库名分组表
func (tbs Tables) GroupByDBName() (m map[string]Tables) {
	m = make(map[string]Tables)
	for _, t := range tbs {
		if _, ok := m[t.DBName]; !ok {
			m[t.DBName] = make(Tables, 0)
		}
		m[t.DBName] = append(m[t.DBName], t)
	}
	return m
}

func (t Table) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s
}

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

type Column struct {
	ColumnName    string   `json:"columnName"`
	DBType        string   `json:"dbType"`
	GoType        string   `json:"goType"`
	Comment       string   `json:"comment"`
	Size          int      `json:"size"`
	Nullable      bool     `json:"nullable,string"`
	Enums         []string `json:"enums"`
	AutoIncrement bool     `json:"autoIncrement,string"`
	PrimaryKey    bool     `json:"primaryKey,string"`
	UniqKey       bool     `json:"uniqKey,string"`
	DefaultValue  string   `json:"defaultValue"`
	OnUpdate      bool     `json:"onUpdate,string"`
	Unsigned      bool     `json:"unsigned,string"`
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

// IsDefaultValueCurrentTimestamp 判断默认值是否为自动填充时间
func (c *Column) IsDefaultValueCurrentTimestamp() bool {

	return strings.Contains(strings.ToLower(c.DefaultValue), DEFAULT_VALUE_CURRENT_TIMESTAMP) // 测试发现有 current_timestamp() 情况
}
