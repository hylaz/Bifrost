package server

import (
	"github.com/brokercap/Bifrost/config"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"sync"
)

type Channel struct {
	sync.RWMutex
	Name             string
	ChanName         chan *outputDriver.PluginDataType
	MaxThreadNum     int
	CurrentThreadNum int
	Status           StatusFlag
	Db               *db
	CountChan        chan *count.FlowCount
}

func NewChannel(maxThreadNum int, name string, db *db) *Channel {
	return &Channel{
		Name:             name,
		ChanName:         make(chan *outputDriver.PluginDataType, maxThreadNum*config.ChannelQueueSize),
		MaxThreadNum:     maxThreadNum,
		CurrentThreadNum: 0,
		Status:           STOPPED,
		Db:               db,
	}
}

func GetChannel(name string, channelId int) *Channel {
	if _, ok := DbList[name]; !ok {
		return nil
	}
	DbList[name].Lock()
	defer DbList[name].Unlock()
	if _, ok := DbList[name].ChannelMap[channelId]; !ok {
		return nil
	}
	return DbList[name].ChannelMap[channelId]
}

func DelChannel(name string, channelId int) bool {
	if _, ok := DbList[name]; !ok {
		return false
	}
	if _, ok := DbList[name].ChannelMap[channelId]; !ok {
		return false
	}
	logrus.Info(DbList[name].Name, "Channel:", DbList[name].ChannelMap[channelId].Name, "delete")
	delete(DbList[name].ChannelMap, channelId)
	return true
}

func (channel *Channel) SetFlowCountChan(flowChan chan *count.FlowCount) {
	channel.CountChan = flowChan
}

func (channel *Channel) GetCountChan() chan *count.FlowCount {
	return channel.CountChan
}

func (channel *Channel) Start() chan *outputDriver.PluginDataType {
	channel.Lock()
	defer channel.Unlock()
	logrus.Info(channel.Db.Name, "Channel:", channel.Name, "start")
	if channel.Status == RUNNING {
		return channel.ChanName
	}

	channel.Status = RUNNING
	for i := 0; i < channel.MaxThreadNum; i++ {
		go channel.channelConsume()
	}
	return channel.ChanName
}

func (channel *Channel) GetChannel() chan *outputDriver.PluginDataType {
	return channel.ChanName
}

func (channel *Channel) Stop() {
	channel.Lock()
	defer channel.Unlock()
	logrus.Info(channel.Db.Name, "Channel:", channel.Name, "stop")
	channel.Status = STOPPED
}

func (channel *Channel) Close() {
	channel.Lock()
	defer channel.Unlock()
	logrus.Info(channel.Db.Name, "Channel:", channel.Name, "close")
	channel.Status = CLOSED
}

func (channel *Channel) SetChannelMaxThreadNum(num int) {
	channel.Lock()
	defer channel.Unlock()
	channel.MaxThreadNum = num
}

func (channel *Channel) GetChannelMaxThreadNum() int {
	channel.Lock()
	defer channel.Unlock()
	return channel.MaxThreadNum
}

func (channel *Channel) channelConsume() {
	channel.Lock()
	defer channel.Unlock()
	channel.CurrentThreadNum++
	defer func() {
		if err := recover(); err != nil {
			logrus.Info("channelConsume err:", err, string(debug.Stack()))
			channel.CurrentThreadNum--
		}
	}()
	newConsumeChannel(channel).consumeChannel()
}
