package server

import (
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
	"time"
)

// Callback 回掉函数
func (db *db) Callback(data *outputDriver.PluginDataType) {
	switch data.EventType {
	case "sql":
		switch data.Query {
		case "COMMIT":
			db.CallbackDoCommit(data)
			return
		case "BEGIN":
			db.lastTransactionTableMap = make(map[string]map[string]bool, 4)
			return
		default:
			break
		}
	case "commit":
		db.CallbackDoCommit(data)
		return
	default:
		break
	}

	if db.Callback0(data) == false {
		return
	}

	if _, ok := db.lastTransactionTableMap[data.SchemaName]; !ok {
		db.lastTransactionTableMap[data.SchemaName] = make(map[string]bool)
	}
	db.lastTransactionTableMap[data.SchemaName][data.TableName] = true
}

func (db *db) CallbackDoCommit(data *outputDriver.PluginDataType) {
	for SchemaName, TableNameMap := range db.lastTransactionTableMap {
		for TableName, _ := range TableNameMap {
			data0 := &outputDriver.PluginDataType{
				Timestamp:       data.Timestamp,
				EventType:       data.EventType,
				SchemaName:      SchemaName,
				TableName:       TableName,
				AliasSchemaName: data.AliasSchemaName,
				AliasTableName:  data.AliasTableName,
				Rows:            data.Rows,
				BinlogFileNum:   data.BinlogFileNum,
				BinlogPosition:  data.BinlogPosition,
				Query:           data.Query,
				Gtid:            data.Gtid,
				Pri:             data.Pri,
				ColumnMapping:   data.ColumnMapping,
				EventID:         data.EventID,
			}
			db.Callback0(data0)
		}
	}
	db.lastTransactionTableMap = make(map[string]map[string]bool, 0)
}

func (db *db) Callback0(data *outputDriver.PluginDataType) (b bool) {
	var channelKey int
	var t *Table
	var getChannelKey = func(SchemaName, tableName string) bool {
		t = db.GetTable(SchemaName, tableName)
		if t == nil {
			return false
		}
		channelKey = t.ChannelKey
		return true
	}

	if getChannelKey("*", "*") {
		b = true
	} else if getChannelKey(data.AliasSchemaName, "*") {
		b = true
	} else if getChannelKey(data.AliasSchemaName, data.AliasTableName) {
		b = true
	} else {
		return
	}

	var i = 0
	var c *Channel
	for {

		if _, ok := db.channelMap[channelKey]; !ok {
			return
		}
		c = db.channelMap[channelKey]
		c.RLock()
		if c.Status == CLOSED {
			c.RUnlock()
			return
		}
		if c.Status != RUNNING {
			c.RUnlock()
			if i%600 == 0 {
				logrus.Printf("channelKey:%T,status:%s,data:%T", channelKey, c.Status, data)
			}
			time.Sleep(1 * time.Second)
			i++
		} else {
			c.RUnlock()
			break
		}
	}

	chanName := c.GetChannel()
	if chanName != nil {
		chanName <- data
	} else {
		logrus.Printf("SchemaName:%s, TableName:%s , ChannelKey:%T chan is nil , data:%T , ", data.AliasSchemaName, data.AliasTableName, channelKey, data)
	}
	return
}
