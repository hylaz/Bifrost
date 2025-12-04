package src

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

type opLog struct {
	Data      *[]dbDriver.Value
	EventType string
}

func (conn *MysqlConn) CommitLogMod_Update(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {

	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)

	//从最后一条数据开始遍历
	var stmt dbDriver.Stmt
	n := len(list)
LOOP:
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value, conn.p.fieldCount*2)
			for i, v := range conn.p.Field {
				var toV dbDriver.Value
				toV, conn.err = conn.dataTypeTransfer(conn.getMySQLData(data, 1, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if conn.err != nil {
					if !conn.p.BifrostMustBeSuccess {
						conn.err = nil
						continue LOOP
					}
					return data
				}
				val[i] = toV
				//第几个字段 + 总字段数量 - 1  算出，on update 所在数组中的位置
				val[i+conn.p.fieldCount] = toV
			}

			if checkOpMap(opMap, data.Rows[1][conn.p.fromPriKey], "update") == true {
				continue
			}
			stmt = conn.getStmt(UPDATE)
			if stmt == nil {
				return data
			}
			_, conn.db.err = stmt.Exec(val)
			if conn.db.err != nil {
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				log.Println("plugin mysql update exec err:", conn.db.err, " data:", val)
				return data
			}
			setOpMapVal(opMap, data.Rows[1][conn.p.fromPriKey], nil, "update")
			break
		case "delete":
			val := make([]dbDriver.Value, conn.p.fieldCount*2)
			for i, v := range conn.p.Field {
				var toV dbDriver.Value
				toV, conn.err = conn.dataTypeTransfer(conn.getMySQLData(data, 0, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if conn.err != nil {
					if !conn.p.BifrostMustBeSuccess {
						conn.err = nil
						continue LOOP
					}
					return data
				}
				val[i] = toV
				//第几个字段 + 总字段数量 - 1  算出，on update 所在数组中的位置
				val[i+conn.p.fieldCount] = toV
			}
			if checkOpMap(opMap, data.Rows[0][conn.p.fromPriKey], "delete") == false {
				stmt = conn.getStmt(UPDATE)
				if stmt == nil {
					return data
				}
				_, conn.db.err = stmt.Exec(val)
				if conn.db.err != nil {
					if conn.CheckDataSkip(data) {
						conn.db.err = nil
						continue LOOP
					}
					log.Println("plugin mysql update exec err:", conn.db.err, " data:", val)
					return data
				}
				setOpMapVal(opMap, data.Rows[0][conn.p.fromPriKey], nil, "delete")
			}
			break
		case "insert":
			val := make([]dbDriver.Value, 0)
			i := 0
			for _, v := range conn.p.Field {
				var toV dbDriver.Value
				toV, conn.err = conn.dataTypeTransfer(conn.getMySQLData(data, 0, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if conn.err != nil {
					if !conn.p.BifrostMustBeSuccess {
						conn.err = nil
						continue LOOP
					}
					return data
				}
				val = append(val, toV)
				i++
			}

			if checkOpMap(opMap, data.Rows[0][conn.p.fromPriKey], "insert") == true {
				continue
			}
			stmt = conn.getStmt(REPLACE_INSERT)
			if stmt == nil {
				return data
			}
			_, conn.db.err = stmt.Exec(val)
			if conn.db.err != nil {
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				log.Println("plugin mysql insert exec err:", conn.db.err, " data:", val)
				return data
			}
			setOpMapVal(opMap, data.Rows[0][conn.p.fromPriKey], &val, "insert")
			break
		}

	}
	return
}
