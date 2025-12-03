package history

import (
	"fmt"
	"github.com/brokercap/Bifrost/mysql"
	"github.com/brokercap/Bifrost/server"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type History struct {
	sync.RWMutex
	ID                 int
	DbName             string
	SchemaName         string
	TableName          string
	Property           HistoryProperty
	Status             HisotryStatus
	NowStartI          uint64 //当前第几条数据
	ThreadPool         []*ThreadStatus
	threadResultChan   chan int        `json:"-"`
	Fields             []TableStruct   `json:"-"`
	TableInfo          TableInfoStruct `json:"-"`
	Uri                string          `json:"-"`
	ToServerIDList     []int
	StartTime          string
	OverTime           string
	TablePriKeyMinId   uint64 // 假如主键是自增id的情况下 这个值是当前自增id最小值
	TablePriKeyMaxId   uint64 // 假如主键是自增id的情况下 这个值是当前自增id最大值
	TablePriKey        string // 主键字段
	TablePriArr        []string
	ToServerList       []*toServer
	ToServerTheadCount int16 // 实际正在运行的同步协程数
	ToServerTheadGroup *WaitGroup
	TableNames         string            // 用 ; 隔开的表名
	TableNameArr       []*TableStatus    // TableNames 分割后的数组
	CurrentTableName   string            // 正在执行全量的表名
	TableCount         int               // 要全量的总表数量
	TableCountSuccess  int               // 已经成功的表数量
	selectStatus       bool              // 拉数据协程状态，true 为已拉完
	SelectRowsCount    uint64            // 成功拉取多少条数据
	ColumnMapping      map[string]string // 表字段类型

	cronEntryID    cron.EntryID  // 定时任务模块返回的ID
	cronStatus     HisotryStatus // 定时任务是否启动
	ContabNextTime time.Time     // 定时任务下一次运行时间

}

func (history *History) InitToServer() {
	history.Lock()
	defer history.Unlock()
	if len(history.ToServerList) > 0 {
		return
	}
	dbSouceInfo := server.GetDBObj(history.DbName)
	Key := server.GetSchemaAndTableJoin(history.SchemaName, history.TableName)
	for _, toServerInfo := range dbSouceInfo.GetTableSelf(history.SchemaName, history.TableName).ToServerList {
		for _, ID := range history.ToServerIDList {
			if ID == toServerInfo.ToServerID {
				toServerInfoNew := &server.ToServer{
					Key:                &Key,
					ToServerID:         0,
					PluginName:         toServerInfo.PluginName,
					MustBeSuccess:      toServerInfo.MustBeSuccess,
					FilterQuery:        toServerInfo.FilterQuery,
					FilterUpdate:       toServerInfo.FilterUpdate,
					FieldList:          toServerInfo.FieldList,
					ToServerKey:        toServerInfo.ToServerKey,
					BinlogFileNum:      toServerInfo.BinlogFileNum,
					BinlogPosition:     toServerInfo.BinlogPosition,
					PluginParam:        toServerInfo.PluginParam,
					Status:             "",
					ToServerChan:       nil,
					Error:              "",
					ErrorWaitDeal:      0,
					ErrorWaitData:      nil,
					LastBinlogFileNum:  0,     // 由 channel 提交到 ToServerChan 的最后一个位点
					LastBinlogPosition: 0,     // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的
					LastBinlogKey:      nil,   // 将数据保存到 level 的key
					QueueMsgCount:      0,     // 队列里的堆积的数量
					FileQueueStatus:    false, // 是否启动文件队列
					Notes:              "history",
				}
				history.ToServerList = append(history.ToServerList, &toServer{threadCount: 0, ToServerInfo: toServerInfoNew})
				break
			}
		}
	}
}

func (history *History) SyncWaitToServerOver(n int) {
	history.Lock()
	defer history.Unlock()
	if history.ToServerTheadGroup != nil {
		history.ToServerTheadGroup.Add(n)
		return
	}
	history.ToServerTheadGroup = NewWaitGroup(n)
	go func() {
		defer func() {
			history.Lock()
			defer history.Unlock()
			history.ToServerTheadGroup = nil
			switch history.Status {
			case HISTORY_STATUS_SELECT_STOPING:
				history.Status = HISTORY_STATUS_SELECT_STOPED
				break
			case HISTORY_STATUS_KILLED, HISTORY_STATUS_HALFWAY:
				break
			default:
				history.Status = HISTORY_STATUS_OVER
				break
			}
		}()
		for {
			history.ToServerTheadGroup.Wait()
			history.Lock()
			if history.selectStatus == true {
				history.Unlock()
				break
			}
			history.Unlock()
			<-time.NewTimer(time.Duration(1) * time.Second).C
		}

	}()
}

func (history *History) Run() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()

	history.Lock()
	if history.cronEntryID == 0 {
		history.Unlock()
		return
	}
	history.Unlock()
	_ = history.Start()
}

func (history *History) LogError(errContent string) {
	logrus.Printf("[ERROR] history task ID:%d DbName:%s SchemaName:%s Table:%s ToServerIDList:%+v %s ", history.ID, history.DbName, history.SchemaName, history.TableNames, history.ToServerIDList, errContent)
}

func (history *History) LogInfo(infoContent string) {
	logrus.Printf("[ERROR] history task ID:%d DbName:%s SchemaName:%s Table:%s ToServerIDList:%+v %s ", history.ID, history.DbName, history.SchemaName, history.TableNames, history.ToServerIDList, infoContent)
}

func (history *History) Start() error {
	history.Lock()

	history.LogInfo("start")
	history.selectStatus = false
	switch history.Status {
	case HISTORY_STATUS_SELECT_STOPING:
		history.Unlock()
		history.LogError("is stoping")
		return fmt.Errorf("is stoping")
	case HISTORY_STATUS_RUNNING:
		history.Unlock()
		history.LogError("is running")
		return fmt.Errorf("is running")
	case HISTORY_STATUS_SELECT_STOPED:
		//
	case HISTORY_STATUS_HALFWAY:
		history.NowStartI = 0
	case HISTORY_STATUS_OVER, HISTORY_STATUS_SELECT_OVER:
		history.TableCountSuccess = 0
		history.NowStartI = 0
	default:
		history.NowStartI = 0
	}

	history.StartTime = time.Now().Format("2006-01-02 15:04:05")
	history.Status = HISTORY_STATUS_RUNNING
	history.NowStartI = 0
	history.SelectRowsCount = 0
	history.Fields = make([]TableStruct, 0)
	history.ThreadPool = make([]*ThreadStatus, history.Property.ThreadNum)
	history.threadResultChan = make(chan int, 1)
	history.ToServerList = make([]*toServer, 0)
	history.OverTime = ""
	history.Unlock()

	go func() {
		defer func() {
			history.Lock()
			defer history.Unlock()
			history.OverTime = time.Now().Format("2006-01-02 15:04:05")
			for _, v := range history.ThreadPool {
				if v.Error != nil {
					history.Status = HISTORY_STATUS_HALFWAY
				}
			}
			if len(history.ToServerList) > 0 {
				history.ToServerList = nil
			}
			if history.Status != HISTORY_STATUS_HALFWAY && history.Status != HISTORY_STATUS_OVER && history.Status != HISTORY_STATUS_SELECT_STOPING && history.Status != HISTORY_STATUS_SELECT_STOPED {
				history.Status = HISTORY_STATUS_SELECT_OVER
			}
			if history.SelectRowsCount == 0 {
				history.Status = HISTORY_STATUS_OVER
			}
			history.selectStatus = true
		}()

		for i, _ := range history.TableNameArr {
			history.TableNameArr[i].SelectCount = 0
		}

		for {
			history.CurrentTableName = history.TableNameArr[history.TableCountSuccess].TableName
			history.Lock()
			switch history.Status {
			case HISTORY_STATUS_HALFWAY, HISTORY_STATUS_SELECT_STOPING, HISTORY_STATUS_SELECT_STOPED, HISTORY_STATUS_KILLED:
				history.Unlock()
				return
			default:
				break
			}

			history.NowStartI = 0
			history.Unlock()
			var selectThreadWg sync.WaitGroup
			for i := 1; i <= history.Property.ThreadNum; i++ {
				selectThreadWg.Add(1)
				go history.threadStart(i-1, &selectThreadWg)
			}
			selectThreadWg.Wait()
			for _, v := range history.ThreadPool {
				if v.Error != nil {
					history.Status = HISTORY_STATUS_HALFWAY
				}
			}

			history.Lock()
			switch history.Status {
			case HISTORY_STATUS_HALFWAY, HISTORY_STATUS_SELECT_STOPING, HISTORY_STATUS_SELECT_STOPED, HISTORY_STATUS_KILLED:
				history.Unlock()
				return
			default:
				break
			}
			history.TableNameArr[history.TableCountSuccess].RowsCount = history.TableNameArr[history.TableCountSuccess].SelectCount
			history.TableCountSuccess++
			history.Unlock()
			if history.TableCountSuccess >= history.TableCount {
				break
			}
			history.Fields = make([]TableStruct, 0)
		}
	}()

	return nil
}

func (history *History) initMetaInfo(db mysql.MysqlConnection) {
	history.Lock()
	defer history.Unlock()
	if len(history.Fields) > 0 {
		return
	}

	history.TablePriKey = ""
	var isCk bool
	var err error
	if isCk, err = IsClickHouse(db); err != nil {
		history.LogError("check is clickhouse error")
		return
	}
	if !isCk {
		history.TableInfo = GetSchemaTableInfo(db, history.SchemaName, history.CurrentTableName)
	}

	//修改表记录总数，用于界面显示
	history.TableNameArr[history.TableCountSuccess].RowsCount = history.TableInfo.TABLE_ROWS

	history.Fields, err = GetSchemaTableFieldList(db, history.SchemaName, history.CurrentTableName, isCk)
	if err != nil {
		history.LogError(fmt.Sprintf("CurrentTableName:%s get schema table fields error:%+v ", history.CurrentTableName, err))
		return
	}
	history.TablePriArr = make([]string, 0)
	history.ColumnMapping = make(map[string]string, 0)
	for _, v := range history.Fields {
		if strings.ToUpper(*v.COLUMN_KEY) == "PRI" {
			history.TablePriArr = append(history.TablePriArr, *v.COLUMN_NAME)
		}
		var columnMappingType string
		switch *v.DATA_TYPE {
		case "tinyint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint8"
			} else {
				if *v.COLUMN_TYPE == "tinyint(1)" {
					columnMappingType = "bool"
				} else {
					columnMappingType = "int8"
				}
			}
		case "smallint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint16"
			} else {
				columnMappingType = "int16"
			}
		case "mediumint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint24"
			} else {
				columnMappingType = "int24"
			}
		case "int":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint32"
			} else {
				columnMappingType = "int32"
			}
		case "bigint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint64"
			} else {
				columnMappingType = "int64"
			}
		case "numeric":
			columnMappingType = strings.Replace(*v.COLUMN_TYPE, "numeric", "decimal", 1)
		case "real":
			columnMappingType = strings.Replace(*v.COLUMN_TYPE, "real", "double", 1)
		case "Int8":
			columnMappingType = "int8"
		case "UInt8":
			columnMappingType = "uint8"
		case "Int16":
			columnMappingType = "int16"
		case "UInt16":
			columnMappingType = "uint16"
		case "Int32":
			columnMappingType = "int32"
		case "UInt32":
			columnMappingType = "uint32"
		case "Int64":
			columnMappingType = "int64"
		case "UInt64":
			columnMappingType = "uint64"
		case "Bool":
			columnMappingType = "bool"
		case "Float32":
			columnMappingType = "float"
		case "Float64":
			columnMappingType = "double"
		default:
			if strings.Contains(*v.COLUMN_TYPE, "Decimal") {
				columnMappingType = strings.Replace(*v.COLUMN_TYPE, "Decimal", "decimal", 1)
				break
			}
			if strings.Index(*v.COLUMN_TYPE, "Array") == 0 {
				columnMappingType = "json"
				break
			}
			if strings.Index(*v.COLUMN_TYPE, "Map") == 0 {
				columnMappingType = "json"
				break
			}
			columnMappingType = *v.COLUMN_TYPE
			break
		}
		if v.IS_NULLABLE != nil && *v.IS_NULLABLE != "NO" {
			columnMappingType = "Nullable(" + columnMappingType + ")"
		}
		history.ColumnMapping[*v.COLUMN_NAME] = columnMappingType
	}
	//假如只有一个主键并且主键自增的情况，找出这个主键最小值和最大值，只支持 无符号的数字。有符号的不支持
	if len(history.TablePriArr) > 0 {
		for _, v := range history.Fields {
			if strings.ToUpper(*v.COLUMN_KEY) == "PRI" && strings.ToLower(*v.EXTRA) == "auto_increment" {
				history.TablePriKeyMinId, history.TablePriKeyMaxId = GetTablePriKeyMinAndMaxVal(db, history.SchemaName, history.CurrentTableName, *v.COLUMN_NAME, history.Property.Where)
				history.TablePriKey = *v.COLUMN_NAME
				break
			}
		}
	}
	// 重新赋值在界面配置的 LimitOptimize 初始值
	history.Property.LimitOptimize = history.Property.FirstLimitOptimize
	// 没有主键的情况下,不能使用 between 等方式查询
	if history.TablePriKey == "" {
		history.Property.LimitOptimize = 0
	}
	// 当总数小于100万的时候的时候，并且自增id 最大值和最小值 差值 的分页数  是 直接 limit 分页数的 2 倍以上的时候，采用常规 limit 分页
	if history.Property.Where == "" && history.Property.LimitOptimize == 1 && history.TableInfo.TABLE_ROWS <= 1000000 && (history.TablePriKeyMaxId-history.TablePriKeyMinId)/uint64(history.Property.ThreadCountPer) > history.TableInfo.TABLE_ROWS/uint64(history.Property.ThreadCountPer)*2 {
		logrus.Println("history", history.DbName, history.SchemaName, history.CurrentTableName, history.ID, " TABLE_ROWS: ", history.TableInfo.TABLE_ROWS, " <= 1000000 ,then transfer LIMIT x,y")
		history.Property.LimitOptimize = 0
	}
	return
}
