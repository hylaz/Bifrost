package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func (conn *MysqlConn) StarRocksCommit_Append(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	var err error
	errData, err = conn.StarRocksInsert(list)
	if err != nil {
		conn.err, conn.db.err = err, err
	} else {
		conn.err = nil
	}
	return errData
}
