package mongo

import (
	"fmt"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/rwynn/gtm/v2"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

func (c *MongoInput) ToInputCallback(op *gtm.Op) {
	if c.callback == nil {
		return
	}
	var data *outputDriver.PluginDataType
	if op.IsCommand() {
		var ok bool
		if _, ok = op.IsDropDatabase(); ok {
			data = c.BuildDropDatabaseQueryEvent(op)
		} else if _, ok = op.IsDropCollection(); ok {
			data = c.BuildDropTableQueryEvent(op)
		} else {
			return
		}
	} else {
		data = c.BuildRowEvent(op)
	}
	if data == nil {
		return
	}
	c.callback(data)
	commitEventData := c.BuildCommitEvent(data)
	c.callback(commitEventData)
}

func (c *MongoInput) BuildRowEvent(op *gtm.Op) *outputDriver.PluginDataType {
	schemaName := op.GetDatabase()
	tableName := op.GetCollection()
	var rows = make([]map[string]interface{}, 0)
	var eventType string
	var ok bool
	var docId primitive.ObjectID
	if docId, ok = op.Id.(primitive.ObjectID); !ok {
		logrus.Printf("[ERROR] MongoInput BuildRowEvent database:%s table:%s _id:%+v (%+v),is not primitive.ObjectID", schemaName, tableName, op.Id, reflect.TypeOf(op.Id))
		return nil
	}
	switch op.Operation {
	case "i":
		eventType = "insert"
		op.Data["_id"] = docId.Hex()
		rows = append(rows, op.Data)
		break
	case "u":
		eventType = "update"
		if op.Data == nil {
			logrus.Printf("[WARN] input[%s] BuildRowEvent[update] _id:%s data is nil \n", "mongo", docId.Hex())
			op.Data = map[string]interface{}{"_id": docId.Hex()}
		} else {
			op.Data["_id"] = docId.Hex()
		}
		rows = append(rows, op.Data, op.Data)
		break
	case "d":
		eventType = "delete"
		if op.Data == nil {
			op.Data = map[string]interface{}{"_id": docId.Hex()}
		} else {
			op.Data["_id"] = docId.Hex()
		}
		rows = append(rows, op.Data)
		break
	default:
		return nil
	}
	return &outputDriver.PluginDataType{
		Timestamp:       op.Timestamp.T,
		EventSize:       0,
		EventType:       eventType,
		Rows:            rows,
		SchemaName:      schemaName,
		TableName:       tableName,
		AliasSchemaName: schemaName,
		AliasTableName:  tableName,
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.OpLogPosition2GTID(&op.Timestamp),
		Pri:             []string{"_id"},
		EventID:         c.getNextEventID(),
		ColumnMapping:   c.TransferDataAndColumnMapping(op.Data),
	}
}

func (c *MongoInput) TransferDataAndColumnMapping(row map[string]interface{}) (columnMapping map[string]string) {
	if row == nil {
		return nil
	}
	columnMapping = make(map[string]string, len(row))
	for key, val := range row {
		if key == "_id" {
			// _id 是主键，所以不能为 Nullable
			columnMapping[key] = "string"
			continue
		}
		if val == nil {
			columnMapping[key] = "Nullable(string)"
			continue
		}
		switch reflect.TypeOf(val).Kind() {
		case reflect.Int8:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(int8)"
			break
		case reflect.Uint8:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(uint8)"
			break
		case reflect.Int16:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(int16)"
			break
		case reflect.Uint16:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(uint16)"
			break
		case reflect.Int32:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(int32)"
			break
		case reflect.Uint32:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(uint32)"
			break
		case reflect.Int, reflect.Int64:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(int64)"
			break
		case reflect.Uint, reflect.Uint64:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(uint64)"
			break
		case reflect.Float32:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(float32)"
			break
		case reflect.Float64:
			row[key] = fmt.Sprint(val)
			columnMapping[key] = "Nullable(float64)"
			break
		case reflect.Bool:
			columnMapping[key] = "Nullable(bool)"
			break
		case reflect.Map, reflect.Array, reflect.Slice:
			columnMapping[key] = "Nullable(json)"
			break
		default:
			switch val.(type) {
			case time.Time:
				// bifrost 重写了 outputDriver.PluginDataType json 序列化，当前不支持 time.Time 格式的反序列化，
				// 并且当前  plugin ck等插件也还不支持time.Time 类型，会被强制转成String格式，但2006-01-02 15:04:05格式字符串会转成timestamp类型
				row[key] = val.(time.Time).Format("2006-01-02 15:04:05")
				columnMapping[key] = "Nullable(timestamp)"
				break
			default:
				columnMapping[key] = "Nullable(string)"
				break
			}
			break
		}
	}
	return
}

func (c *MongoInput) BuildDropDatabaseQueryEvent(op *gtm.Op) *outputDriver.PluginDataType {
	sql := fmt.Sprintf("DROP DATABASE %s", op.GetDatabase())
	return c.BuildQueryEvent(op, sql)
}

func (c *MongoInput) BuildDropTableQueryEvent(op *gtm.Op) *outputDriver.PluginDataType {
	sql := fmt.Sprintf("DROP TABLE %s", op.GetCollection())
	return c.BuildQueryEvent(op, sql)
}

func (c *MongoInput) BuildQueryEvent(op *gtm.Op, sql string) *outputDriver.PluginDataType {
	schemaName := op.GetDatabase()
	tableName := op.GetCollection()
	return &outputDriver.PluginDataType{
		Timestamp:       op.Timestamp.T,
		EventSize:       uint32(len(sql)),
		EventType:       "sql",
		Rows:            nil,
		Query:           sql,
		SchemaName:      schemaName,
		TableName:       tableName,
		AliasSchemaName: schemaName,
		AliasTableName:  tableName,
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.OpLogPosition2GTID(&op.Timestamp),
		Pri:             nil,
		EventID:         c.getNextEventID(),
	}
}

func (c *MongoInput) BuildCommitEvent(data *outputDriver.PluginDataType) *outputDriver.PluginDataType {
	return &outputDriver.PluginDataType{
		Timestamp:       data.Timestamp,
		EventSize:       5,
		EventType:       "commit",
		Rows:            nil,
		Query:           "",
		SchemaName:      data.SchemaName,
		TableName:       data.TableName,
		AliasSchemaName: data.AliasSchemaName,
		AliasTableName:  data.AliasTableName,
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            data.Gtid,
		Pri:             nil,
		EventID:         c.getNextEventID(),
	}
}
