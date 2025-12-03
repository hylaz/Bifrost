package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"strings"
)

// 最上层回调 已经加锁

func (c *InputKafka) SetTopicPartitionOffsetAndReturnGTID(kafkaMsg *sarama.ConsumerMessage) (GTID string) {
	if kafkaMsg != nil {
		var ok bool
		if _, ok = c.positionMap[kafkaMsg.Topic]; !ok {
			c.positionMap[kafkaMsg.Topic] = make(map[int32]int64, 0)
		}
		c.positionMap[kafkaMsg.Topic][kafkaMsg.Partition] = kafkaMsg.Offset
	}
	return c.positionMapToGTID(c.positionMap)
}

func (c *InputKafka) positionMapToGTID(positionMap map[string]map[int32]int64) (GTID string) {
	var grids = make([]string, 0)
	for topic, p := range positionMap {
		for partition, offset := range p {
			grids = append(grids, fmt.Sprintf("%s:%d:%d", topic, partition, offset))
		}
	}
	return strings.Join(grids, ",")
}
