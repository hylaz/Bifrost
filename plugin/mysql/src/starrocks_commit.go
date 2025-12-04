package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
)

func (conn *MysqlConn) StarrocksNotAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	conn.db.err = conn.db.Begin()
	if conn.db.err != nil {
		return nil, conn.db.err
	}
	switch conn.p.SyncMode {
	case SYNCMODE_NORMAL:
		ErrData = conn.CommitNormal(list)
		break
	case SYNCMODE_LOG_UPDATE:
		ErrData = conn.CommitLogMod_Update(list)
		break
	case SYNCMODE_LOG_APPEND:
		ErrData = conn.CommitLogMod_Append(list)
		break
	default:
		conn.err = fmt.Errorf("同步模式ERROR:%s", conn.p.SyncMode)
		break
	}
	if conn.db.err != nil {
		conn.err = conn.db.err
		return ErrData, conn.err
	}
	if conn.err != nil {
		conn.db.err = conn.db.Rollback()
		logrus.Println("plugin mysql err", conn.err)
		return ErrData, conn.err
	}
	conn.db.err = conn.db.Commit()
	conn.StmtClose()
	if conn.db.err != nil {
		return nil, conn.db.err
	}
	return
}

// 自动创建表的提交
func (conn *MysqlConn) StarrocksAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, data := range dataMap {
		p, err := conn.getAutoTableFieldType(data[0])
		if err != nil {
			return data[0], e
		}
		conn.p.Field = p.Field
		conn.p.fieldCount = len(p.Field)
		conn.p.schemaAndTable = p.SchemaAndTable
		conn.p.PriKey = p.PriKey
		conn.p.toPriKey = p.ToPriKey
		conn.p.fromPriKey = p.FromPriKey
		conn.db.err = conn.db.Begin()
		if conn.db.err != nil {
			conn.err = conn.db.err
			break
		}
		switch conn.p.SyncMode {
		case SYNCMODE_NORMAL:
			ErrData = conn.CommitNormal(data)
			break
		case SYNCMODE_LOG_UPDATE:
			ErrData = conn.CommitLogMod_Update(data)
			break
		case SYNCMODE_LOG_APPEND:
			ErrData = conn.CommitLogMod_Append(data)
			break
		default:
			conn.err = fmt.Errorf("同步模式ERROR:%s", conn.p.SyncMode)
			break
		}
		if conn.db.err != nil {
			conn.err = conn.db.err
		}
		if conn.err != nil {
			conn.db.err = conn.db.Rollback()
			return ErrData, conn.err
		}
		conn.db.err = conn.db.Commit()
		conn.StmtClose()
		if conn.db.err != nil {
			break
		}
	}
	return
}
