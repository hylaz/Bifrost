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
	chanName         chan *outputDriver.PluginDataType
	MaxThreadNum     int
	CurrentThreadNum int
	Status           StatusFlag
	db               *db
	countChan        chan *count.FlowCount
}

func NewChannel(MaxThreadNum int, Name string, db *db) *Channel {
	return &Channel{
		Name:             Name,
		chanName:         make(chan *outputDriver.PluginDataType, MaxThreadNum*config.ChannelQueueSize),
		MaxThreadNum:     MaxThreadNum,
		CurrentThreadNum: 0,
		Status:           STOPPED,
		db:               db,
	}
}

func GetChannel(name string, channelID int) *Channel {
	if _, ok := DbList[name]; !ok {
		return nil
	}
	DbList[name].Lock()
	defer DbList[name].Unlock()
	if _, ok := DbList[name].channelMap[channelID]; !ok {
		return nil
	}
	return DbList[name].channelMap[channelID]
}

func DelChannel(name string, channelID int) bool {
	if _, ok := DbList[name]; !ok {
		return false
	}
	if _, ok := DbList[name].channelMap[channelID]; !ok {
		return false
	}
	logrus.Println(DbList[name].Name, "Channel:", DbList[name].channelMap[channelID].Name, "delete")
	delete(DbList[name].channelMap, channelID)
	return true
}

func (channel *Channel) SetFlowCountChan(flowChan chan *count.FlowCount) {
	channel.countChan = flowChan
}

func (channel *Channel) GetCountChan() chan *count.FlowCount {
	return channel.countChan
}

func (channel *Channel) Start() chan *outputDriver.PluginDataType {
	channel.Lock()
	defer channel.Unlock()
	logrus.Println(channel.db.Name, "Channel:", channel.Name, "start")
	if channel.Status == RUNNING {
		return channel.chanName
	}

	channel.Status = RUNNING
	for i := 0; i < channel.MaxThreadNum; i++ {
		go channel.channelConsume()
	}
	return channel.chanName
}

func (channel *Channel) GetChannel() chan *outputDriver.PluginDataType {
	return channel.chanName
}

func (channel *Channel) Stop() {
	channel.Lock()
	defer channel.Unlock()
	logrus.Println(channel.db.Name, "Channel:", channel.Name, "stop")
	channel.Status = STOPPED
}

func (channel *Channel) Close() {
	channel.Lock()
	defer channel.Unlock()
	logrus.Println(channel.db.Name, "Channel:", channel.Name, "close")
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
			logrus.Println("channelConsume err:", err, string(debug.Stack()))
			channel.CurrentThreadNum--
		}
	}()
	newConsumeChannel(channel).consumeChannel()
}
