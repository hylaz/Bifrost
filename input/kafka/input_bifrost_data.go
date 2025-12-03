package kafka

import (
	"encoding/json"
	"github.com/IBM/sarama"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func init() {
	inputDriver.Register("bifrost_kafka", NewBifrostDataInput, Version, BifrostVersion)
}

type BifrostDataInput struct {
	InputKafka
}

func NewBifrostDataInput() inputDriver.Driver {
	c := &BifrostDataInput{}
	c.Init()
	c.childCallBack = c.CallBack
	return c
}

func (c *BifrostDataInput) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	var data outputDriver.PluginDataType
	c.err = json.Unmarshal(kafkaMsg.Value, &data)
	if c.err != nil {
		return c.err
	}
	data.Gtid = c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg)
	data.EventSize = uint32(len(kafkaMsg.Value))
	data.BinlogFileNum = 1
	data.BinlogPosition = 0
	data.EventID = c.getNextEventID()
	data.AliasSchemaName = kafkaMsg.Topic
	data.AliasTableName = c.FormatPartitionTableName(kafkaMsg.Partition)
	c.ToInputCallback(&data)
	return nil
}
