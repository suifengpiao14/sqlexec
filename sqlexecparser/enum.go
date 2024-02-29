package sqlexecparser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type Enum struct {
	Key   string `json:"key"`   // 枚举类型定义 常量 名称
	Value string `json:"value"` // 枚举类型定义值
	Title string `json:"title"` // 枚举类型 标题（中文）
	Type  string `json:"type"`  // 类型 int-整型，string-字符串，默认string
}

type Enums []*Enum

func (e Enums) Len() int { // 重写 Len() 方法
	return len(e)
}
func (e Enums) Swap(i, j int) { // 重写 Swap() 方法
	e[i], e[j] = e[j], e[i]
}
func (e Enums) Less(i, j int) bool { // 重写 Less() 方法， 从小到大排序
	return e[i].Key < e[j].Key
}

// UniqueItems 去重
func (e Enums) UniqueItems() (uniq Enums) {
	emap := make(map[string]*Enum)
	for _, enum := range e {
		emap[enum.Key] = enum
	}
	uniq = Enums{}
	for _, enum := range emap {
		uniq = append(uniq, enum)
	}
	return
}

//ParserEnum 解析枚举值
func ParserEnum(column *Column) (enumsConsts Enums) {
	prefix := fmt.Sprintf("%s_%s", column.TableName, column.ColumnName)
	enumsConsts = Enums{}
	comment := strings.ReplaceAll(column.Comment, " ", ",") // 替换中文逗号(兼容空格和逗号另种分割符号)
	reg, err := regexp.Compile(`\W`)
	if err != nil {
		panic(err)
	}
	for _, constValue := range column.Enums {
		constKey := fmt.Sprintf("%s_%s", prefix, constValue)
		valueFormat := fmt.Sprintf("%s-", constValue) // 枚举类型的comment 格式 value1-title1,value2-title2
		index := strings.Index(comment, valueFormat)
		if index < 0 {
			err := errors.Errorf("column %s(enum) comment except contains %s-xxx,got:%s", column.ColumnName, constValue, comment)
			panic(err)
		}
		title := comment[index+len(valueFormat):]
		comIndex := strings.Index(title, ",")
		if comIndex > -1 {
			title = title[:comIndex]
		} else {
			title = strings.TrimRight(title, " )")
		}
		constKey = reg.ReplaceAllString(constKey, "_") //替换非字母字符
		constKey = strings.ToUpper(constKey)
		enumsConst := &Enum{
			Key:   constKey,
			Value: constValue,
			Title: title,
			Type:  "string",
		}
		enumsConsts = append(enumsConsts, enumsConst)
	}
	return
}
