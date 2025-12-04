package src

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
)

func (conn *MysqlConn) CommitLogMod_Append(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	//将update, delete,insert 的数据全转成  insert 语句
	var stmt dbDriver.Stmt
	n := len(list)
LOOP:
	for i := 0; i < n; i++ {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value, 0)
			for _, v := range conn.p.Field {
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
				val = append(val, toV)
			}
			stmt = conn.getStmt(INSERT)
			if stmt == nil {
				return data
			}
			_, conn.db.err = stmt.Exec(val)
			if conn.db.err != nil {
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				logrus.Println("plugin mysql insert exec err:", conn.db.err, " data:", val)
				return data
			}
			break
		case "insert", "delete":
			val := make([]dbDriver.Value, 0)
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
			}
			stmt = conn.getStmt(INSERT)
			if stmt == nil {
				return data
			}
			_, conn.db.err = stmt.Exec(val)
			if conn.db.err != nil {
				if conn.CheckDataSkip(data) {
					conn.db.err = nil
					continue LOOP
				}
				logrus.Println("plugin mysql insert exec err:", conn.db.err, " data:", val)
				return data
			}
			break
		}

	}
	return
}
