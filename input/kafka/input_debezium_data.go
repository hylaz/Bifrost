package kafka

import (
	"github.com/IBM/sarama"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func init() {
	inputDriver.Register("debezium_kafka", NewDebeziumDataInput, Version, BifrostVersion)
}

type DebeziumDataInput struct {
	InputKafka
}

func NewDebeziumDataInput() inputDriver.Driver {
	c := &DebeziumDataInput{}
	c.Init()
	c.childCallBack = c.CallBack
	return c
}

func (c *DebeziumDataInput) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	debezium, err := outputDriver.NewDebezium(kafkaMsg.Key, kafkaMsg.Value)
	if err != nil {
		return err
	}
	if debezium == nil {
		return nil
	}
	data := debezium.ToBifrostOutputPluginData()
	data.Gtid = c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg)
	data.EventSize = uint32(len(kafkaMsg.Value))
	data.BinlogFileNum = 1
	data.BinlogPosition = 0
	data.EventID = c.getNextEventID()
	data.AliasSchemaName = kafkaMsg.Topic
	data.AliasTableName = c.FormatPartitionTableName(kafkaMsg.Partition)
	c.ToInputCallback(data)
	return nil
}
