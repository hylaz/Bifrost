package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"sync"
)

//一个input可以设置多个协程进行处理反序列化解析的数据,同时需要保证同一个partition的数据只能被一个协程处理

func (c *InputKafka) InitInputCosume(workerCount int) *sync.WaitGroup {
	ws := &sync.WaitGroup{}
	ws.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		ch := make(chan *sarama.ConsumerMessage, 5000)
		c.inputCosumeList = append(c.inputCosumeList, ch)
		go func(workerId int) {
			defer ws.Done()
			c.InputCosume(c.kafkaGroupCtx, workerId, ch)
		}(i)
	}
	return ws
}

// 当前这个方法,主要用于单测,实际业务代码中,没有场景使用到
// 假如中途close了chan,异步协程是有可能会写入失败的
func (c *InputKafka) CloseInputCosume() {
	if len(c.inputCosumeList) == 0 {
		return
	}
	for i := range c.inputCosumeList {
		close(c.inputCosumeList[i])
	}
}

func (c *InputKafka) SendToInputConsume(kafkaMsg *sarama.ConsumerMessage) {
	crc32Int := c.CRC32KafkaMsgTopicAndPartition(kafkaMsg)
	i := crc32Int % len(c.inputCosumeList)
	select {
	case c.inputCosumeList[i] <- kafkaMsg:
		break
	case <-c.kafkaGroupCtx.Done():
		break
	}
}

func (c *InputKafka) CRC32KafkaMsgTopicAndPartition(kafkaMsg *sarama.ConsumerMessage) int {
	key := fmt.Sprintf("%s_%d", kafkaMsg.Topic, kafkaMsg.Partition)
	crc32Int := int(crc32.ChecksumIEEE([]byte(key)))
	return crc32Int
}

func (c *InputKafka) InputCosume(ctx context.Context, workerId int, ch chan *sarama.ConsumerMessage) {
	logrus.Printf("[INFO] output[%s] InputCosume workeId:%d starting\n", "kafka", workerId)
	defer logrus.Printf("[INFO] output[%s] InputCosume workeId:%d end\n", "kafka", workerId)
	for {
		select {
		case <-ctx.Done():
			return
		case kafkaMsg := <-ch:
			if kafkaMsg == nil {
				return
			}
			c.ToChildCallback(kafkaMsg)
			break
		}
	}
}
