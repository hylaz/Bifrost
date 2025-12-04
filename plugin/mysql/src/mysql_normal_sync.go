package src

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
)

func (conn *MysqlConn) CommitNormal(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {

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
					if conn.CheckDataSkip(data) {
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
				logrus.Println("plugin mysql update exec err:", conn.db.err, " data:", val)
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				return data
			}
			setOpMapVal(opMap, data.Rows[1][conn.p.fromPriKey], nil, "update")
			break
		case "delete":
			where := make([]dbDriver.Value, 0)
			for _, v := range conn.p.PriKey {
				var toV dbDriver.Value
				toV, conn.err = conn.dataTypeTransfer(conn.getMySQLData(data, 0, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if conn.err != nil {
					if !conn.p.BifrostMustBeSuccess {
						conn.err = nil
						continue LOOP
					}
					if conn.CheckDataSkip(data) {
						conn.err = nil
						continue LOOP
					}
					return data
				}
				where = append(where, toV)
			}
			if checkOpMap(opMap, data.Rows[0][conn.p.fromPriKey], "delete") == false {
				stmt = conn.getStmt(DELETE)
				if stmt == nil {
					return data
				}
				_, conn.db.err = stmt.Exec(where)
				if conn.db.err != nil {
					logrus.Println("plugin mysql delete exec err:", conn.db.err, " where:", where)
					if conn.CheckDataSkip(data) {
						conn.db.err = nil
						continue LOOP
					}
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
					if conn.CheckDataSkip(data) {
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
				logrus.Println("plugin mysql insert exec err:", conn.db.err, " data:", val)
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				return data
			}
			setOpMapVal(opMap, data.Rows[0][conn.p.fromPriKey], &val, "insert")
			break
		}
	}
	return
}
