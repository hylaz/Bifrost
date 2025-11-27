package server

import "strings"

var dbAndTableSplitChars = "_-"

func GetSchemaAndTableJoin(schema, tableName string) string {
	return schema + dbAndTableSplitChars + tableName
}

func GetSchemaAndTableBySplit(schemaAndTableName string) (schemaName, tableName string) {
	var i int
	// 这里这么操作 是因为 最开始设计 的时候是用  - 分割，现在发现 有不少用户 库名也有 -
	// 为了兼容 ， 这里先判断一下 -, 是否存在，假如哪个用户 库名和表名都有 - 这个时候就会有问题了，但愿没这样的用户嘿嘿
	i = strings.Index(schemaAndTableName, dbAndTableSplitChars)
	if i == -1 {
		if strings.Count(schemaAndTableName, "-") > 1 {
			i = strings.LastIndexAny(schemaAndTableName, "-")
		} else {
			i = strings.IndexAny(schemaAndTableName, "-")
		}
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+1:]
	} else {
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+2:]
	}
	return
}
