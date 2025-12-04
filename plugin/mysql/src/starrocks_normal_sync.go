package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
)

func (conn *MysqlConn) StarRocksCommitNormal(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	var err error
	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)
	insertList := make([]*pluginDriver.PluginDataType, 0)
	deleteList := make([][]string, 0)
	//从最后一条数据开始遍历
	n := len(list)
LOOP:
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		if conn.CheckDataSkip(data) {
			conn.db.err = nil
			continue LOOP
		}
		switch data.EventType {
		case "update", "insert":
			k := len(data.Rows) - 1
			if checkOpMap(opMap, data.Rows[k][conn.p.fromPriKey], data.EventType) == true {
				continue
			}
			setOpMapVal(opMap, data.Rows[k][conn.p.fromPriKey], nil, data.EventType)
			insertList = append(insertList, data)
			break
		case "delete":
			priKey := data.Rows[0][conn.p.fromPriKey]
			if priKey == nil {
				continue
			}
			if checkOpMap(opMap, data.Rows[0][conn.p.fromPriKey], "delete") == false {
				setOpMapVal(opMap, data.Rows[0][conn.p.fromPriKey], nil, "delete")
				deleteList = append(deleteList, []string{fmt.Sprint(priKey)})
			}
			break
		default:
			continue
		}
	}
	if len(deleteList) > 0 {
		err = conn.StarRocksDelete(conn.GetSchemaName(list[0]), conn.GetTableName(list[0]), list[0].Pri, deleteList)
		if err != nil {
			conn.err, conn.db.err = err, err
			logrus.Printf("[ERROR] output[%s] StarRocksCommitNormal delete:(%+v) SchemaName:%s TableName:%s err:%+v", OutputName, deleteList, list[0].SchemaName, list[0].TableName, err)
			return nil
		} else {
			conn.err = nil
		}
	}
	if len(insertList) > 0 {
		errData, err = conn.StarRocksInsert(insertList)
		if err != nil {
			conn.err, conn.db.err = err, err
			logrus.Printf("[ERROR] output[%s] StarRocksCommitNormal insert SchemaName:%s TableName:%s err:%+v", OutputName, list[0].SchemaName, list[0].TableName, err)
			return errData
		} else {
			conn.err = nil
		}
	}
	return nil
}
