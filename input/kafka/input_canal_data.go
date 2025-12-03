package kafka

import (
	"github.com/IBM/sarama"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func init() {
	inputDriver.Register("canal_kafka", NewCanalDataInput, Version, BifrostVersion)
}

type CanalDataInput struct {
	InputKafka
}

func NewCanalDataInput() inputDriver.Driver {
	c := &CanalDataInput{}
	c.Init()
	c.childCallBack = c.CallBack
	return c
}

func (c *CanalDataInput) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	canal, err := outputDriver.NewPluginDataCanal(kafkaMsg.Value)
	if err != nil {
		return err
	}
	data := canal.ToBifrostOutputPluginData()
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
