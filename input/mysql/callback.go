package mysql

import (
	"fmt"
	mysql2 "github.com/brokercap/Bifrost/mysql"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"strconv"
	"strings"
)

func evenTypeName(e mysql2.EventType) string {
	switch e {
	case mysql2.WRITE_ROWS_EVENTv0, mysql2.WRITE_ROWS_EVENTv1, mysql2.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql2.UPDATE_ROWS_EVENTv0, mysql2.UPDATE_ROWS_EVENTv1, mysql2.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql2.DELETE_ROWS_EVENTv0, mysql2.DELETE_ROWS_EVENTv1, mysql2.DELETE_ROWS_EVENTv2:
		return "delete"
	case mysql2.QUERY_EVENT:
		return "sql"
	case mysql2.XID_EVENT:
		return "commit"
	default:
		break
	}
	return fmt.Sprintf("%d", e)
}

func (c *MysqlInput) MySQLCallback(data *mysql2.EventReslut) {
	if c.callback == nil {
		return
	}
	c.eventID = data.EventID
	i := strings.IndexAny(data.BinlogFileName, ".")
	intString := data.BinlogFileName[i+1:]
	BinlogFileNum, _ := strconv.Atoi(intString)
	data0 := &pluginDriver.PluginDataType{
		EventSize:       data.Header.EventSize,
		Timestamp:       data.Header.Timestamp,
		EventType:       evenTypeName(data.Header.EventType),
		SchemaName:      data.SchemaName,
		TableName:       data.TableName,
		AliasSchemaName: data.SchemaName,
		AliasTableName:  data.TableName,
		Rows:            data.Rows,
		BinlogFileNum:   BinlogFileNum,
		BinlogPosition:  data.Header.LogPos,
		Query:           data.Query,
		Gtid:            data.Gtid,
		Pri:             data.Pri,
		ColumnMapping:   data.ColumnMapping,
		EventID:         data.EventID,
	}
	c.callback(data0)
}
