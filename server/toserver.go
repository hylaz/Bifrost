package server

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/filequeue"
	"sync"
)

type ToServerStatus string

type ToServer struct {
	sync.RWMutex
	Key               string `json:"-"` // 上一级的key
	ToServerID        int
	PluginName        string
	MustBeSuccess     bool
	FilterQuery       bool
	FilterUpdate      bool
	FieldList         []string
	ToServerKey       string
	LastSuccessBinlog *PositionStruct // 最后处理成功的位点信息
	LastQueueBinlog   *PositionStruct // 最后进入队列的位点信息
	BinlogFileNum     int             // 支持到 1.8.x
	BinlogPosition    uint32          // 支持到 1.8.x
	PluginParam       map[string]interface{}
	Status            StatusFlag
	ToServerChan      *ToServerChan `json:"-"`

	Error         string
	ErrorWaitDeal int
	ErrorWaitData *pluginDriver.PluginDataType

	LastBinlogFileNum  int    // 由 channel 提交到 ToServerChan 的最后一个位点 // 将会在 1.8.x 版本开始去掉这个字段
	LastBinlogPosition uint32 // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的  // 支持到 1.8.x

	LastBinlogKey                 []byte `json:"-"` // 将数据保存到 level 的key
	QueueMsgCount                 uint32 // 队列里的堆积的数量
	FileQueueObj                  *filequeue.Queue
	FileQueueStatus               bool // 是否启动文件队列
	Notes                         string
	ThreadCount                   int16  // 消费线程数量
	FileQueueUsableCount          uint32 // 在开始文件队列的配置下，每次写入 ToServerChan 后 ，在 FileQueueUsableCountTimeDiff 时间内 队列都是满的次数
	FileQueueUsableCountStartTime int64  // 开始统计 FileQueueUsableCount 计算的时间
	StatusChan                    chan bool
	ConsumePluginParamArr         []interface{} `json:"-"` // 用以区分多个消费者的身份
}

func (server *ToServer) UpdateBinlogPosition(binlogFileNum int, binlogPosition uint32, gtid string, timestamp uint32) bool {
	server.Lock()
	defer server.Unlock()
	server.LastSuccessBinlog = &PositionStruct{
		BinlogFileNum:  binlogFileNum,
		BinlogPosition: binlogPosition,
		GTID:           gtid,
		Timestamp:      timestamp,
		EventID:        0,
	}
	return true
}

func (server *ToServer) AddWaitError(waitErr error, waitData *pluginDriver.PluginDataType) bool {
	server.Lock()
	defer server.Unlock()
	server.Error = waitErr.Error()
	server.ErrorWaitData = waitData
	return true
}

func (server *ToServer) DealWaitError() bool {
	server.Lock()
	defer server.Unlock()
	server.ErrorWaitDeal = 1
	return true
}

func (server *ToServer) GetWaitErrorDeal() int {
	server.Lock()
	deal := server.ErrorWaitDeal
	server.Unlock()
	return deal
}

func (server *ToServer) DelWaitError() bool {
	server.Lock()
	defer server.Unlock()
	server.Error = ""
	server.ErrorWaitData = nil
	server.ErrorWaitDeal = 0
	return true
}
