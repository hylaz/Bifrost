package server

import (
	"fmt"
	"sync"
)

type Table struct {
	sync.RWMutex
	Key            string // schema+table 组成的key
	Name           string
	ChannelKey     int
	LastToServerID int
	ToServerList   []*ToServer
	LikeTableList  []*Table        // 关联了哪些 模糊匹配的配置
	RegexpErr      bool            // 是否执行正则表达式错误，如果 true，则下一次不会再执行，直接错过
	IgnoreTable    string          // 假如是模糊匹配的时候，指定某些表不进行匹配，逗号隔开
	IgnoreTableMap map[string]bool // 指定某些表不进行匹配的表数据 map 格式
	DoTable        string
	DoTableMap     map[string]bool // 指定某些表进行匹配的表数据 map 格式
}

func AddTable(db, schema, tableName, IgnoreTable string, DoTable string, channelId int) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + " not exsit")
	}
	if DbList[db].AddTable(schema, tableName, IgnoreTable, DoTable, channelId, 0) {
		return nil
	}
	return fmt.Errorf("unkown error")
}

func UpdateTable(db, schema, tableName, IgnoreTable string, DoTable string) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + " not exsit")
	}
	if DbList[db].UpdateTable(schema, tableName, IgnoreTable, DoTable) {
		return nil
	}
	return fmt.Errorf("unkown error")
}

func DelTable(db, schema, tableName string) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + "not exsit")
	}
	DbList[db].DelTable(schema, tableName)
	return nil
}

func AddTableToServer(db, schemaName, tableName string, ToServerInfo ToServer) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + "not exsit")
	}
	key := GetSchemaAndTableJoin(schemaName, tableName)
	if _, ok := DbList[db].tableMap[key]; !ok {
		return fmt.Errorf(key + " not exsit")
	} else {
		DbList[db].AddTableToServer(schemaName, tableName, &ToServerInfo)
	}
	return nil
}
