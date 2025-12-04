package src

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

func (rabbitmqConn *RabbitmqConn) SendAndWait(exchange string, routingKey string, body []byte, DeliveryMode uint8) (bool, error) {
	ch := rabbitmqConn.getChannel(true)
	if ch == nil {
		rabbitmqConn.status = "close"
		return false, rabbitmqConn.err
	}
	err := ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		true,       // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: DeliveryMode,
			Expiration:   rabbitmqConn.p.expir,
		})
	if err != nil {
		rabbitmqConn.err = err
		rabbitmqConn.status = "close"
		return false, err
	}
	timer := time.NewTimer(10 * time.Second)
	select {
	case d := <-rabbitmqConn.confirmWait:
		if d.DeliveryTag >= 0 {
			timer.Stop()
			return true, nil
		}
		rabbitmqConn.err = fmt.Errorf("unkonw err")
		break
	case <-timer.C:
		rabbitmqConn.err = fmt.Errorf("server no response")
		break
	}
	timer.Stop()
	rabbitmqConn.status = "close"
	return false, rabbitmqConn.err
}

func (rabbitmqConn *RabbitmqConn) SendAndNoWait(exchange string, routingkey string, body []byte, DeliveryMode uint8) (bool, error) {
	ch := rabbitmqConn.getChannel(false)
	if ch == nil {
		rabbitmqConn.status = "close"
		return false, rabbitmqConn.err
	}
	err := ch.Publish(
		exchange,   // exchange
		routingkey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: DeliveryMode,
			Expiration:   rabbitmqConn.p.expir,
		})
	if err != nil {
		rabbitmqConn.err = err
		rabbitmqConn.status = "close"
		return false, err
	}
	return true, nil
}
