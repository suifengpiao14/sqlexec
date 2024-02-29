package sqlexecparser

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
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
	DBName      DBName      `json:"dbName"` // 额外增加
	TableName   TableName   `json:"tableName"`
	Columns     Columns     `json:"columns"`
	Comment     string      `json:"comment"`
	Constraints Constraints `json:"constraints"`
}

func (t Table) Fullname() (fullname string) {
	fullname = fmt.Sprintf("`%s`.`%s`", t.DBName.Base(), t.TableName.Base())
	return fullname
}

type Tables []Table

func (a Tables) Len() int           { return len(a) }
func (a Tables) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Tables) Less(i, j int) bool { return a[i].Fullname() < a[j].Fullname() }

func (tbs Tables) String() string {
	b, err := json.Marshal(tbs)
	if err != nil {
		panic(err)
	}
	s := string(b)
	return s
}

//根据库名分组表
func (tbs Tables) GroupByDBName() (m map[DBName]Tables) {
	m = make(map[DBName]Tables)
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
	Type        string       `json:"type"`
	ColumnNames []ColumnName `json:"columnNames"`
}

const (
	Constraint_Type_Primary  = "primary"
	Constraint_Type_Uniqueue = "uniqueue"
)

func (c *Constraint) AddColumnName(columnNames ...ColumnName) {
	if c.ColumnNames == nil {
		c.ColumnNames = make([]ColumnName, 0)
	}
	for _, columnName := range columnNames {
		exists := false
		for _, cName := range c.ColumnNames {

			if cName.EqualFold(columnName) {
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

func (c Constraint) IsSubSet(eq bool, columnNames ...ColumnName) (yes bool) {
	if eq && len(columnNames) != len(c.ColumnNames) { // 相等时，检测长度
		return false
	}
	for _, name := range columnNames {
		exists := false
		for _, en := range c.ColumnNames {
			if en.EqualFold(name) {
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

func (cs *Constraints) Add(typ string, columnNames ...ColumnName) {
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

func (cs Constraints) IsPrimaryKeyPart(columnNames ...ColumnName) (yes bool) {
	primaryConstraint, ok := cs.GetByType(Constraint_Type_Primary)
	if !ok {
		return false
	}
	yes = primaryConstraint.IsSubSet(false, columnNames...)
	return yes
}
func (cs Constraints) IsPrimaryKey(columnNames ...ColumnName) (yes bool) {
	primaryConstraint, ok := cs.GetByType(Constraint_Type_Primary)
	if !ok {
		return false
	}
	yes = primaryConstraint.IsSubSet(true, columnNames...)
	return yes
}

func (cs Constraints) IsUniqKeyPart(columnNames ...ColumnName) (yes bool) {
	primaryConstraint, ok := cs.GetByType(Constraint_Type_Uniqueue)
	if !ok {
		return false
	}
	yes = primaryConstraint.IsSubSet(false, columnNames...)
	return yes
}
func (cs Constraints) IsUniqKey(columnNames ...ColumnName) (yes bool) {
	primaryConstraint, ok := cs.GetByType(Constraint_Type_Uniqueue)
	if !ok {
		return false
	}
	yes = primaryConstraint.IsSubSet(true, columnNames...)
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

type DBName string

func (t DBName) Base() (dbName string) {
	s := string(t)
	dbName = strings.ReplaceAll(s, "`", "")
	return dbName
}

func (dname DBName) EqualFold(dbName DBName) (ok bool) {
	tableNameStr := string(dbName)
	tableNameStr = strings.ReplaceAll(tableNameStr, "`", "")
	tnameStr := string(dname)
	tnameStr = strings.ReplaceAll(tnameStr, "`", "")
	return strings.EqualFold(tnameStr, tableNameStr)
}

type TableName string

func (tname TableName) EqualFold(tableName TableName) (ok bool) {
	tableNameStr := string(tableName)
	tableNameStr = strings.ReplaceAll(tableNameStr, "`", "")
	tnameStr := string(tname)
	tnameStr = strings.ReplaceAll(tnameStr, "`", "")
	return strings.EqualFold(tnameStr, tableNameStr)
}

func (t TableName) Explain() (dbName, tableName string) {
	s := string(t)
	arr := strings.Split(strings.ReplaceAll(s, "`", ""), ".")
	switch len(arr) {
	case 1:
		tableName = arr[0]
	case 2:
		dbName, tableName = arr[0], arr[1]
	}
	return dbName, tableName
}

func (t TableName) Base() (tableName string) {
	_, tableName = t.Explain()
	return tableName
}

//SqlparserColName 转成sqlParser 对象
func (t TableName) SqlparserColName() (tabName *sqlparser.TableName) {
	dbName, tableName := t.Explain()
	tabName = &sqlparser.TableName{Name: sqlparser.NewTableIdent(tableName)}
	if dbName != "" {
		tabName.Qualifier = sqlparser.NewTableIdent(dbName)
	}
	return tabName
}

type ColumnName string

func ToColumnName(strs ...string) (cns []ColumnName) {
	cns = make([]ColumnName, 0)
	for _, s := range strs {
		cns = append(cns, ColumnName(s))
	}
	return cns
}

func (c ColumnName) EqualFold(colName ColumnName) (ok bool) {
	colNameStr := string(colName)
	colNameStr = strings.ReplaceAll(colNameStr, "`", "")
	cStr := string(c)
	cStr = strings.ReplaceAll(cStr, "`", "")
	return strings.EqualFold(cStr, colNameStr)
}

//Explain 展开名称的各部分
func (c ColumnName) Explain() (dbName, tableName, columnName string) {
	s := string(c)
	arr := strings.Split(strings.ReplaceAll(s, "`", ""), ".")
	switch len(arr) {
	case 1:
		columnName = arr[0]
	case 2:
		tableName, columnName = arr[0], arr[1]
	case 3:
		dbName, tableName, columnName = arr[0], arr[1], arr[2]
	}
	return dbName, tableName, columnName
}

func (c ColumnName) Base() (columnName string) {
	_, _, columnName = c.Explain()
	return columnName
}

//SqlparserColName 转成sqlParser 对象
func (c ColumnName) SqlparserColName() (colName *sqlparser.ColName) {
	colName = &sqlparser.ColName{}
	dbName, tableName, columnName := c.Explain()
	colName.Name = sqlparser.NewColIdent(columnName)
	if tableName != "" {
		colName.Qualifier = sqlparser.TableName{Name: sqlparser.NewTableIdent(tableName)}
	}
	if dbName != "" {
		colName.Qualifier.Qualifier = sqlparser.NewTableIdent(dbName)
	}
	return colName
}

type Column struct {
	DBName        DBName     `json:"dbName"`
	TableName     TableName  `json:"tableName"`
	ColumnName    ColumnName `json:"columnName"`
	DBType        string     `json:"dbType"`
	GoType        string     `json:"goType"`
	Comment       string     `json:"comment"`
	Size          int        `json:"size"`
	Nullable      bool       `json:"nullable,string"`
	Enums         []string   `json:"enums"`
	AutoIncrement bool       `json:"autoIncrement,string"`
	PrimaryKey    bool       `json:"primaryKey,string"`
	UniqKey       bool       `json:"uniqKey,string"`
	DefaultValue  string     `json:"defaultValue"`
	OnUpdate      bool       `json:"onUpdate,string"`
	Unsigned      bool       `json:"unsigned,string"`
}

func (c Column) ColumnFullname() (fullname string) {
	fullname = fmt.Sprintf("%s.%s.%s", c.DBName, c.TableName, c.ColumnName)
	fullname = strings.Trim(fullname, ".")
	return fullname
}

func (c Column) TableFullname() (fullname string) {
	fullname = fmt.Sprintf("%s.%s", c.DBName, c.TableName)
	fullname = strings.Trim(fullname, ".")
	return fullname
}

type Columns []Column

func (cs Columns) GetFirst() (first *Column, ok bool) {
	if len(cs) == 0 {
		return nil, false
	}
	return &cs[0], true
}
func (cs Columns) GetByName(name ColumnName) (column *Column, ok bool) {
	for _, c := range cs {
		if name.EqualFold(c.ColumnName) {
			return &c, true
		}
	}
	return nil, false
}

func (cs Columns) GetByNames(names ...ColumnName) (columns Columns, err error) {
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

//GetNames 获取所有的列 名称
func (cs Columns) GetNames() (columnNames []ColumnName) {
	columnNames = make([]ColumnName, 0)
	for _, c := range cs {
		columnNames = append(columnNames, c.ColumnName)
	}
	return columnNames
}

// IsDefaultValueCurrentTimestamp 判断默认值是否为自动填充时间
func (c *Column) IsDefaultValueCurrentTimestamp() bool {

	return strings.Contains(strings.ToLower(c.DefaultValue), DEFAULT_VALUE_CURRENT_TIMESTAMP) // 测试发现有 current_timestamp() 情况
}
