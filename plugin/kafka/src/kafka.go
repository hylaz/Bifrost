package src

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"time"
)

const Version = "v2.0.5"
const BifrostVersion = "v2.0.5"

func init() {
	pluginDriver.Register("kafka", NewKafkaConn, Version, BifrostVersion)
}

const (
	Runing int8 = 1
	Closed int8 = 0
)

type KafkaConn struct {
	pluginDriver.PluginDriverInterface
	Uri      string
	status   int8
	err      error
	p        *PluginParam
	producer sarama.SyncProducer
}

type PluginParam struct {
	OtherObjectType      pluginDriver.OtherObjectType
	Topic                string
	Key                  string
	BatchSize            int
	Timeout              int
	RequiredAcks         sarama.RequiredAcks
	BifrostFilterQuery   bool // bifrost server 保留,是否过滤sql事件
	BifrostMustBeSuccess bool // bifrost server 保留,数据是否能丢
	dataList             []*sarama.ProducerMessage
	commitBinlogList     []*pluginDriver.PluginDataType
}

func NewKafkaConn() pluginDriver.Driver {
	f := &KafkaConn{
		status: Closed,
	}
	return f
}

func (conn *KafkaConn) SetOption(uri *string, param map[string]interface{}) {
	conn.Uri = *uri
	return
}

func (conn *KafkaConn) Open() error {
	conn.Connect()
	return nil
}

func (conn *KafkaConn) GetUriExample() string {
	return "127.0.0.1:9092,127.0.0.1:9093"
}

func (conn *KafkaConn) CheckUri() error {
	config, err := getKafkaConnectConfig(parseDSN(conn.Uri))
	if err != nil {
		conn.err = err
		return err
	}
	config.ConnectConfig.Producer.Return.Successes = true
	config.ConnectConfig.Producer.Return.Errors = true
	producer, err := sarama.NewSyncProducer(config.BrokerServerList, config.ConnectConfig)
	if err == nil {
		producer.Close()
	}
	return err
}

func (conn *KafkaConn) newProducer() bool {
	config, err := getKafkaConnectConfig(parseDSN(conn.Uri))
	if err != nil {
		return false
	}
	config.ConnectConfig.Producer.Return.Successes = true
	config.ConnectConfig.Producer.Return.Errors = true
	config.ConnectConfig.Producer.RequiredAcks = conn.p.RequiredAcks
	config.ConnectConfig.Producer.Timeout = time.Duration(conn.p.Timeout) * time.Second
	conn.producer, conn.err = sarama.NewSyncProducer(config.BrokerServerList, config.ConnectConfig)
	if conn.err == nil {
		conn.status = Runing
		return true
	} else {
		return false
	}
}

func (conn *KafkaConn) Connect() bool {
	conn.err = fmt.Errorf("no producer")
	conn.status = Closed
	return true
}

func (conn *KafkaConn) GetParam(p interface{}) (interface{}, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	param := &PluginParam{RequiredAcks: -1, Timeout: 10}
	err = json.Unmarshal(s, param)
	if err != nil {
		return nil, err
	}
	if param.BatchSize <= 0 {
		param.BatchSize = 1
	}
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.Timeout < 0 {
		param.Timeout = 0
	}
	switch param.RequiredAcks {
	case sarama.NoResponse, sarama.WaitForAll, sarama.WaitForLocal:
		break
	default:
		param.RequiredAcks = sarama.WaitForAll
		break
	}
	if len(param.dataList) == 0 {
		param.dataList = make([]*sarama.ProducerMessage, 0)
		param.commitBinlogList = make([]*pluginDriver.PluginDataType, 0)
	}
	conn.p = param
	return param, nil
}

func (conn *KafkaConn) SetParam(p any) (any, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		conn.p = p.(*PluginParam)
		return p, nil
	default:
		return conn.GetParam(p)
	}
}

func (conn *KafkaConn) ReConnect() bool {
	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		if conn.producer != nil {
			conn.producer.Close()
		}
	}()

	r := conn.newProducer()
	if r {
		return true
	} else {
		return false
	}
}

func (conn *KafkaConn) Close() bool {
	if conn.producer != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			conn.producer.Close()
		}()
	}
	conn.producer = nil
	conn.status = Closed
	return true
}

func (conn *KafkaConn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(data, retry, false)
}

func (conn *KafkaConn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(data, retry, false)
}

func (conn *KafkaConn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(data, retry, false)
}

func (conn *KafkaConn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(data, retry, false)
}

func (conn *KafkaConn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(data, retry, true)
}

func (conn *KafkaConn) getMsg(data *pluginDriver.PluginDataType) (*sarama.ProducerMessage, error) {
	Topic := fmt.Sprint(pluginDriver.TransfeResult(conn.p.Topic, data, len(data.Rows)-1))
	msg := &sarama.ProducerMessage{}
	msg.Topic = Topic
	if conn.p.Key != "" {
		Key := fmt.Sprint(pluginDriver.TransfeResult(conn.p.Key, data, len(data.Rows)-1))
		msg.Key = sarama.StringEncoder(Key)
	}
	toOtherObjectTypeData, _ := pluginDriver.ToOtherObject(data, conn.p.OtherObjectType)
	c, err := json.Marshal(toOtherObjectTypeData)
	if err != nil {
		return nil, err
	}
	msg.Value = sarama.StringEncoder(c)
	return msg, nil
}

func (conn *KafkaConn) sendToList(data *pluginDriver.PluginDataType, retry bool, isCommit bool) (LastSuccessCommitData *pluginDriver.PluginDataType, Errdata *pluginDriver.PluginDataType, err error) {
	if data == nil && retry {
		LastSuccessCommitData, err = conn.sendToKafkaByBatch()
		goto endErr
	}

	if conn.p.BatchSize > 1 {
		if !retry {
			var msg *sarama.ProducerMessage
			if !isCommit || !conn.p.BifrostFilterQuery {
				msg, err = conn.getMsg(data)
				if err != nil {
					goto endErr
				}
				conn.p.dataList = append(conn.p.dataList, msg)
			}

			if isCommit {
				n0 := len(conn.p.dataList) / conn.p.BatchSize
				if len(conn.p.commitBinlogList)-1 < n0 {
					conn.p.commitBinlogList = append(conn.p.commitBinlogList, data)
				} else {
					conn.p.commitBinlogList[n0] = data
				}
			}
		}

		if len(conn.p.dataList) >= conn.p.BatchSize {
			LastSuccessCommitData, err = conn.sendToKafkaByBatch()
		}
	} else {
		if isCommit && conn.p.BifrostFilterQuery {
			return LastSuccessCommitData, nil, nil
		}
		var msg *sarama.ProducerMessage
		msg, err = conn.getMsg(data)
		if err != nil {
			goto endErr
		}
		if conn.status != Runing {
			conn.ReConnect()
			if conn.status != Runing {
				err = conn.err
				goto endErr
			}
		}
		_, _, err = conn.producer.SendMessage(msg)
		if err == nil {
			LastSuccessCommitData = data
		}
	}
endErr:
	if err != nil {
		if !conn.p.BifrostMustBeSuccess {
			return LastSuccessCommitData, nil, nil
		}
		if conn.err != nil {
			conn.status = Closed
			return nil, nil, conn.err
		}
		return nil, nil, err
	}
	return LastSuccessCommitData, nil, nil
}

func (conn *KafkaConn) sendToKafkaByBatch() (*pluginDriver.PluginDataType, error) {
	if conn.status != Runing {
		conn.ReConnect()
		if conn.status != Runing {
			return nil, conn.err
		}
	}

	if len(conn.p.dataList) == 0 {
		return nil, nil
	}

	var err error
	var binlogEvent *pluginDriver.PluginDataType
	if len(conn.p.dataList) > conn.p.BatchSize {
		list := conn.p.dataList[:conn.p.BatchSize]
		err = conn.producer.SendMessages(list)
		if err == nil {
			conn.p.dataList = conn.p.dataList[conn.p.BatchSize:]
			if len(conn.p.commitBinlogList) > 0 {
				binlogEvent = conn.p.commitBinlogList[0]
				conn.p.commitBinlogList = conn.p.commitBinlogList[1:]
			}
		}
	} else {
		err = conn.producer.SendMessages(conn.p.dataList)
		if err == nil {
			conn.p.dataList = make([]*sarama.ProducerMessage, 0)
			if len(conn.p.commitBinlogList) > 0 {
				binlogEvent = conn.p.commitBinlogList[0]
				conn.p.commitBinlogList = conn.p.commitBinlogList[1:]
			}
		}
	}

	if err != nil {
		return nil, err
	}
	if binlogEvent != nil {
		return binlogEvent, nil
	} else {
		return nil, nil
	}
}

func (conn *KafkaConn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToList(nil, true, false)
}
