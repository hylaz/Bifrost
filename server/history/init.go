package history

import (
	"fmt"
	"github.com/brokercap/Bifrost/server"
	"sync"
)

var historyMap map[string]map[int]*History
var lastHistoryID int
var l sync.RWMutex

func init() {
	lastHistoryID = 0
	historyMap = make(map[string]map[int]*History, 0)
}

type HisotryStatus string

const (
	HISTORY_STATUS_ALL            HisotryStatus = "All"
	HISTORY_STATUS_CLOSE          HisotryStatus = "close"
	HISTORY_STATUS_RUNNING        HisotryStatus = "running"
	HISTORY_STATUS_OVER           HisotryStatus = "over"
	HISTORY_STATUS_HALFWAY        HisotryStatus = "halfway"
	HISTORY_STATUS_KILLED         HisotryStatus = "killed"
	HISTORY_STATUS_SELECT_OVER    HisotryStatus = "selectOver" //拉取数据结束
	HISTORY_STATUS_SELECT_STOPING HisotryStatus = "stoping"
	HISTORY_STATUS_SELECT_STOPED  HisotryStatus = "stoped"
)

type HistoryProperty struct {
	ThreadNum          int    // 拉取数据协程数量,每个协程一个连接
	ThreadCountPer     int    // 协程每次最多处理多少条数据
	Where              string // where 条件
	LimitOptimize      int8   // 是否自动分页优化, 1 采用 between 方式优化 0 不启动优化
	SyncThreadNum      int    // 同步协程数
	FirstLimitOptimize int8   // 被添加的时候 LimitOptimize 的值，因为计算的时候，LimitOptimize 是可能被修改掉值
	Crontab            string // 定时表达式，如果为空，则说明没有定时
}

type ThreadStatus struct {
	Num       int
	Error     error  // 拉取数据错误
	NowStartI uint64 // 当前执行第几条
}

type toServer struct {
	sync.RWMutex
	threadCount  int
	ToServerInfo *server.ToServer
}

type TableStatus struct {
	sync.RWMutex
	RowsCount   uint64
	SelectCount uint64
	TableName   string
}

func Start(dbName string, ID int) error {
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}
	return historyMap[dbName][ID].Start()
}
