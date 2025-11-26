package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

func (c *InputKafka) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *InputKafka) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *InputKafka) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	go c.StartConsumePluginPosition(sess)
	for {
		select {
		case kafkaMsg := <-claim.Messages():
			if kafkaMsg == nil {
				return nil
			}
			c.SendToInputConsume(kafkaMsg)
			break
		case _ = <-c.kafkaGroupCtx.Done():
			return nil
		}
	}
	return nil
}

func (c *InputKafka) StartConsumePluginPosition(sess sarama.ConsumerGroupSession) {
	c.Lock()
	if c.consumeClaimCtx != nil {
		c.Unlock()
		return
	}
	c.consumeClaimCtx, c.consumeClaimCancle = context.WithCancel(c.kafkaGroupCtx)
	c.Unlock()
	defer func() {
		c.consumeClaimCtx = nil
		c.consumeClaimCancle = nil
	}()
	defer c.consumeClaimCancle()

	c.ConsumePluginPosition(sess, c.consumeClaimCtx)
}
