package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"strings"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

// 支持整个整个表作为json结构体
func init() {
	inputDriver.Register("table_json_kafka", NewInputTableJsonData, Version, BifrostVersion)
}

type InputTableJsonata struct {
	InputKafka
	columnMapping map[string]string
	pri           []string
	tableName     string
	database      string
}

func NewInputTableJsonData() inputDriver.Driver {
	c := &InputTableJsonata{}
	c.Init()
	c.childCallBack = c.CallBack
	return c
}

func (c *InputTableJsonata) childInit() {
	if len(c.pri) > 0 {
		return
	}
	if c.config == nil {
		return
	}
	if c.config.ParamMap == nil {
		return
	}
	if _, ok := c.config.ParamMap["input.pri"]; ok {
		c.pri = strings.Split(c.config.ParamMap["input.pri"], ",")
	}
	if _, ok := c.config.ParamMap["input.table"]; ok {
		c.tableName = fmt.Sprint(c.config.ParamMap["input.table"])
	}
	if _, ok := c.config.ParamMap["input.database"]; ok {
		c.database = fmt.Sprint(c.config.ParamMap["input.database"])
	}
}

func (c *InputTableJsonata) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	if len(kafkaMsg.Value) == 0 {
		return nil
	}
	c.childInit()
	var msgData map[string]interface{}
	err := json.Unmarshal(kafkaMsg.Value, &msgData)
	if err != nil {
		return err
	}
	var SchemaName, TableName string
	if c.tableName == "" {
		TableName = c.FormatPartitionTableName(kafkaMsg.Partition)
	} else {
		TableName = c.tableName
	}
	if c.database == "" {
		SchemaName = kafkaMsg.Topic
	} else {
		SchemaName = c.database
	}

	data := &outputDriver.PluginDataType{
		Timestamp:       uint32(kafkaMsg.Timestamp.Unix()),
		EventSize:       uint32(len(kafkaMsg.Value)),
		EventType:       "insert",
		Rows:            []map[string]interface{}{msgData},
		Query:           "",
		SchemaName:      SchemaName,
		TableName:       TableName,
		AliasSchemaName: kafkaMsg.Topic,
		AliasTableName:  c.FormatPartitionTableName(kafkaMsg.Partition),
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg),
		EventID:         c.getNextEventID(),
		ColumnMapping:   nil,
		Pri:             c.pri,
	}
	c.ToInputCallback(data)
	return nil
}
