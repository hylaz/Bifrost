package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"strconv"
)

const Version = "v1.6.0"
const BifrostVersion = "v1.0.0"

func init() {
	pluginDriver.Register("rabbitmq", NewRabbitmqConn, Version, BifrostVersion)
}

type RabbitmqConn struct {
	pluginDriver.PluginDriverInterface
	uri         string
	status      string
	conn        *amqp.Connection
	ch          *amqp.Channel
	chNoWait    *amqp.Channel
	confirmWait chan amqp.Confirmation
	p           *PluginParam
	err         error
	queueMap    map[string]bool
	exchangeMap map[string]bool
	bindMap     map[string]bool
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

type PluginParam struct {
	Queue              Queue
	Exchange           Exchange
	Confirm            bool
	Persistent         bool
	RoutingKey         string
	Expir              int
	Declare            bool
	expir              string
	deliveryMode       uint8
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func NewRabbitmqConn() pluginDriver.Driver {
	f := &RabbitmqConn{status: "close"}
	return f
}

func (rabbitmqConn *RabbitmqConn) SetOption(uri *string, param map[string]interface{}) {
	rabbitmqConn.uri = *uri
	return
}

func (rabbitmqConn *RabbitmqConn) Open() error {
	rabbitmqConn.Connect()
	return nil
}

func (rabbitmqConn *RabbitmqConn) GetUriExample() string {
	return "amqp://guest:guest@localhost:5672/MyVhost"
}

func (rabbitmqConn *RabbitmqConn) CheckUri() error {
	rabbitmqConn.Connect()
	if rabbitmqConn.err != nil {
		return rabbitmqConn.err
	}
	rabbitmqConn.Close()
	return nil
}

func (rabbitmqConn *RabbitmqConn) Connect() bool {
	var err error
	rabbitmqConn.conn, err = amqp.Dial(rabbitmqConn.uri)
	if err != nil {
		rabbitmqConn.err = err
		rabbitmqConn.status = "close"
		return false
	}
	rabbitmqConn.queueMap = make(map[string]bool)
	rabbitmqConn.exchangeMap = make(map[string]bool)
	rabbitmqConn.bindMap = make(map[string]bool)
	rabbitmqConn.err = nil
	rabbitmqConn.status = "running"
	return true
}

func (rabbitmqConn *RabbitmqConn) getChannel(confirm bool) *amqp.Channel {
	if confirm {
		if rabbitmqConn.ch == nil {
			rabbitmqConn.ch, rabbitmqConn.err = rabbitmqConn.conn.Channel()
			if rabbitmqConn.err != nil {
				rabbitmqConn.ch = nil
				return nil
			}
			rabbitmqConn.ch.Confirm(false)
			rabbitmqConn.confirmWait = make(chan amqp.Confirmation, 1)
			rabbitmqConn.ch.NotifyPublish(rabbitmqConn.confirmWait)
		}
		return rabbitmqConn.ch
	} else {

		if rabbitmqConn.chNoWait == nil {
			rabbitmqConn.chNoWait, rabbitmqConn.err = rabbitmqConn.conn.Channel()
			if rabbitmqConn.err != nil {
				rabbitmqConn.chNoWait = nil
				return nil
			}
		}
		return rabbitmqConn.chNoWait
	}
}
func (rabbitmqConn *RabbitmqConn) ReConnect() bool {
	rabbitmqConn.Close()
	r := rabbitmqConn.Connect()
	if r == true {
		return true
	} else {
		return false
	}
}

func (rabbitmqConn *RabbitmqConn) Close() bool {
	if rabbitmqConn.conn == nil {
		return true
	}
	func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Println("ReConnect recory:", err)
				return
			}
		}()
		if rabbitmqConn.ch != nil {
			rabbitmqConn.ch.Close()
			rabbitmqConn.ch = nil
		}
		if rabbitmqConn.chNoWait != nil {
			rabbitmqConn.chNoWait.Close()
			rabbitmqConn.chNoWait = nil
		}
		rabbitmqConn.conn.Close()
	}()
	rabbitmqConn.conn = nil
	rabbitmqConn.status = "close"
	rabbitmqConn.err = fmt.Errorf("closed")
	return true
}

func (rabbitmqConn *RabbitmqConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.Expir > 0 {
		param.expir = strconv.Itoa(param.Expir)
	}
	if param.Persistent == true {
		param.deliveryMode = 2
	} else {
		param.deliveryMode = 1
	}
	rabbitmqConn.p = &param
	return &param, nil
}

func (rabbitmqConn *RabbitmqConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		rabbitmqConn.p = p.(*PluginParam)
		return p, nil
	default:
		return rabbitmqConn.GetParam(p)
	}
}

func (rabbitmqConn *RabbitmqConn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return rabbitmqConn.sendToList(data)
}

func (rabbitmqConn *RabbitmqConn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return rabbitmqConn.sendToList(data)
}

func (rabbitmqConn *RabbitmqConn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return rabbitmqConn.sendToList(data)
}

func (rabbitmqConn *RabbitmqConn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return rabbitmqConn.sendToList(data)
}

func (rabbitmqConn *RabbitmqConn) Commit(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	if rabbitmqConn.p.BifrostFilterQuery {
		return data, nil, nil
	}
	LastSuccessCommitData, ErrData, err = rabbitmqConn.sendToList(data)
	if err == nil {
		LastSuccessCommitData = data
	}
	return
}

func (rabbitmqConn *RabbitmqConn) Declare(Queue string, Exchange string, RoutingKey string) error {
	ch := rabbitmqConn.getChannel(rabbitmqConn.p.Confirm)
	if ch == nil {
		rabbitmqConn.status = "close"
		return rabbitmqConn.err
	}
	if _, ok := rabbitmqConn.queueMap[Queue]; !ok {
		p := make(amqp.Table)
		_, err := ch.QueueDeclare(Queue, rabbitmqConn.p.Queue.Durable, rabbitmqConn.p.Queue.AutoDelete, false, false, p)
		if err != nil {
			return err
		}
		rabbitmqConn.queueMap[Queue] = true
	}

	if _, ok := rabbitmqConn.exchangeMap[Exchange]; !ok {
		p := make(amqp.Table)
		err := ch.ExchangeDeclare(Exchange, rabbitmqConn.p.Exchange.Type, rabbitmqConn.p.Exchange.Durable, false, false, false, p)
		if err != nil {
			return err
		}
		rabbitmqConn.exchangeMap[Exchange] = true
	}

	key := Queue + "-" + Exchange + "-" + RoutingKey
	if _, ok := rabbitmqConn.bindMap[key]; !ok {
		p := make(amqp.Table)
		err := ch.QueueBind(Queue, RoutingKey, Exchange, false, p)
		if err != nil {
			return err
		}
		rabbitmqConn.bindMap[key] = true
	}
	return nil
}

func (rabbitmqConn *RabbitmqConn) sendToList(data *pluginDriver.PluginDataType) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if rabbitmqConn.status != "running" {
		rabbitmqConn.ReConnect()
		if rabbitmqConn.status != "running" {
			return nil, data, rabbitmqConn.err
		}
	}
	c, err := json.Marshal(data)
	if err != nil {
		rabbitmqConn.err = err
		return nil, data, err
	}
	var queuename string
	var exchange string
	var routingkey string
	index := len(data.Rows) - 1
	exchange = fmt.Sprint(pluginDriver.TransfeResult(rabbitmqConn.p.Exchange.Name, data, index))
	routingkey = fmt.Sprint(pluginDriver.TransfeResult(rabbitmqConn.p.RoutingKey, data, index))
	if rabbitmqConn.p.Declare == true {
		queuename = fmt.Sprint(pluginDriver.TransfeResult(rabbitmqConn.p.Queue.Name, data, index))
		if err := rabbitmqConn.Declare(queuename, exchange, routingkey); err != nil {
			return nil, data, err
		}
	}
	if rabbitmqConn.p.Confirm {
		_, err = rabbitmqConn.SendAndWait(exchange, routingkey, c, rabbitmqConn.p.deliveryMode)
	} else {
		_, err = rabbitmqConn.SendAndNoWait(exchange, routingkey, c, rabbitmqConn.p.deliveryMode)
	}
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (rabbitmqConn *RabbitmqConn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}
