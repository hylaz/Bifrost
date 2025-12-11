package server

import (
	"context"
	"errors"
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"github.com/brokercap/Bifrost/mysql"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/server/filequeue"
	"github.com/brokercap/Bifrost/server/warning"
	"github.com/sirupsen/logrus"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

var AllSchemaAndTablekey = GetSchemaAndTableJoin("*", "*")

var DbLock sync.Mutex
var DbList map[string]*db

func init() {
	DbList = make(map[string]*db, 0)
}

func GetDb(name string) *db {
	DbLock.Lock()
	defer DbLock.Unlock()
	return DbList[name]
}

func AddNewDb(name, inputType string, inputInfo inputDriver.InputInfo, addTime int64) *db {
	r := false
	DbLock.Lock()
	if _, ok := DbList[name]; !ok {
		DbList[name] = NewDb(name, inputType, inputInfo, addTime)
		r = true
	}
	count.SetDB(name)
	DbLock.Unlock()

	logrus.Println("Add db Info:", inputType, name, inputInfo)
	if r {
		return DbList[name]
	} else {
		return nil
	}

}

func UpdateDb(name, inputType string, inputInfo inputDriver.InputInfo, updateTime int64, updateToServer int8) error {
	DbLock.Lock()
	defer DbLock.Unlock()
	if _, ok := DbList[name]; !ok {
		return fmt.Errorf(name + "not exsit")
	}

	if inputInfo.ServerId == 0 {
		return fmt.Errorf("serverId can't be 0")
	}

	dbObj := DbList[name]
	dbObj.Lock()
	defer dbObj.Unlock()
	if dbObj.ConnStatus != CLOSED {
		return fmt.Errorf("db status must be close")
	}

	dbObj.ConnectUri = inputInfo.ConnectUri
	dbObj.BinlogDumpFileName = inputInfo.BinlogFileName
	dbObj.BinlogDumpPosition = inputInfo.BinlogPostion
	dbObj.ServerId = inputInfo.ServerId
	dbObj.MaxBinlogDumpFileName = inputInfo.MaxFileName
	dbObj.MaxBinlogDumpPosition = inputInfo.MaxPosition
	dbObj.AddTime = updateTime
	if inputInfo.GTID == "" {
		dbObj.Gtid = inputInfo.GTID
		dbObj.IsGtid = false
	} else {
		dbObj.Gtid = inputInfo.GTID
		dbObj.IsGtid = true
	}
	logrus.Println("Update db Info:", inputType, name, inputInfo)
	if updateToServer == 0 {
		return nil
	}

	var BinlogFileNum int
	if inputInfo.BinlogFileName != "" {
		index := strings.Index(inputInfo.BinlogFileName, ".")
		BinlogFileNum, _ = strconv.Atoi(inputInfo.BinlogFileName[index+1:])
	}

	for key, t := range dbObj.TableMap {
		for _, toServer := range t.ToServerList {
			logrus.Println("UpdateToServerBinlogPosition:", key, " QueueMsgCount:", toServer.QueueMsgCount, " old:", toServer.BinlogFileNum, toServer.BinlogPosition, " new:", BinlogFileNum, inputInfo.BinlogPostion)
			toServer.UpdateBinlogPosition(BinlogFileNum, inputInfo.BinlogPostion, inputInfo.GTID, 0)
		}
	}
	return nil
}

func GetDBObj(name string) *db {
	if _, ok := DbList[name]; !ok {
		return nil
	}
	return DbList[name]
}

func DelDb(name string) bool {
	DbLock.Lock()
	defer DbLock.Unlock()
	positionBinlogKey := getBinlogKey(DbList[name])
	if _, ok := DbList[name]; ok {

		if DbList[name].ConnStatus == CLOSED {
			for _, c := range DbList[name].ChannelMap {
				count.DelChannel(name, c.Name)
			}
			delete(DbList, name)
			count.DelDB(name)
			logrus.Println("delete db:", name)
		} else {
			return false
		}
	}

	// 删除binlog 信息
	delBinlogPosition(positionBinlogKey)
	return true
}

type db struct {
	sync.RWMutex
	Name          string            `json:"Name"`
	ConnectUri    string            `json:"ConnectUri"`
	ConnStatus    StatusFlag        `json:"ConnStatus"`
	ConnErr       string            `json:"ConnErr"`
	ChannelMap    map[int]*Channel  `json:"ChannelMap"`
	LastChannelID int               `json:"LastChannelID"`
	TableMap      map[string]*Table `json:"TableMap"`

	IsGtid                  bool             `json:"IsGtid"`
	Gtid                    string           `json:"Gtid"`
	BinlogDumpFileName      string           `json:"BinlogDumpFileName"`
	BinlogDumpPosition      uint32           `json:"BinlogDumpPosition"`
	BinlogDumpTimestamp     uint32           `json:"BinlogDumpTimestamp"`
	LastEventID             uint64           `json:"LastEventID"`
	ReplicateDoDb           map[string]uint8 `json:"ReplicateDoDb"`
	ServerId                uint32           `json:"ServerId"`
	killStatus              int
	MaxBinlogDumpFileName   string `json:"MaxBinlogDumpFileName"`
	MaxBinlogDumpPosition   uint32 `json:"MaxBinlogDumpPosition"`
	AddTime                 int64
	DBBinlogKey             []byte                     `json:"-"` // 保存 binlog到levelDB 的key
	LastTransactionTableMap map[string]map[string]bool `json:"-"` // 最近一个事务里更新了数据表
	InputType               string                     `json:"Name"`
	InputDriverObj          inputDriver.Driver         `json:"-"` // 数据源实例化对象
	InputStatusChan         chan *inputDriver.PluginStatus

	StatusCtx struct {
		ctx       context.Context
		cancelFun context.CancelFunc
	} `json:"-"`
}

type DbListStruct struct {
	Name                  string
	InputType             string
	ConnectUri            string
	ConnStatus            StatusFlag //close,stop,starting,running
	ConnErr               string
	ChannelCount          int
	LastChannelID         int
	TableCount            int
	BinlogDumpFileName    string
	BinlogDumpPosition    uint32
	IsGtid                bool
	Gtid                  string
	BinlogDumpTimestamp   uint32
	LastEventID           uint64
	MaxBinlogDumpFileName string
	MaxBinlogDumpPosition uint32
	ReplicateDoDb         map[string]uint8
	ServerId              uint32
	AddTime               int64
}

func GetListDb() map[string]DbListStruct {
	dbListMap := make(map[string]DbListStruct, 0)
	DbLock.Lock()
	defer DbLock.Unlock()
	for k, v := range DbList {
		dbListMap[k] = DbListStruct{
			Name:                  v.Name,
			InputType:             v.InputType,
			ConnectUri:            v.ConnectUri,
			ConnStatus:            v.ConnStatus,
			ConnErr:               v.ConnErr,
			ChannelCount:          len(v.ChannelMap),
			LastChannelID:         v.LastChannelID,
			TableCount:            len(v.TableMap),
			BinlogDumpFileName:    v.BinlogDumpFileName,
			BinlogDumpPosition:    v.BinlogDumpPosition,
			IsGtid:                v.IsGtid,
			Gtid:                  v.Gtid,
			LastEventID:           v.LastEventID,
			BinlogDumpTimestamp:   v.BinlogDumpTimestamp,
			MaxBinlogDumpFileName: v.MaxBinlogDumpFileName,
			MaxBinlogDumpPosition: v.MaxBinlogDumpPosition,
			ReplicateDoDb:         v.ReplicateDoDb,
			ServerId:              v.ServerId,
			AddTime:               v.AddTime,
		}
	}
	return dbListMap
}

func GetDbInfo(dbname string) *DbListStruct {
	DbLock.Lock()
	defer DbLock.Unlock()
	v := DbList[dbname]
	if v == nil {
		return &DbListStruct{}
	}
	return &DbListStruct{
		Name:                  v.Name,
		InputType:             v.InputType,
		ConnectUri:            v.ConnectUri,
		ConnStatus:            v.ConnStatus,
		ConnErr:               v.ConnErr,
		ChannelCount:          len(v.ChannelMap),
		LastChannelID:         v.LastChannelID,
		TableCount:            len(v.TableMap),
		BinlogDumpFileName:    v.BinlogDumpFileName,
		BinlogDumpPosition:    v.BinlogDumpPosition,
		BinlogDumpTimestamp:   v.BinlogDumpTimestamp,
		Gtid:                  v.Gtid,
		LastEventID:           v.LastEventID,
		MaxBinlogDumpFileName: v.MaxBinlogDumpFileName,
		MaxBinlogDumpPosition: v.MaxBinlogDumpPosition,
		ReplicateDoDb:         v.ReplicateDoDb,
		ServerId:              v.ServerId,
		AddTime:               v.AddTime,
	}
}

func NewDb(Name string, InputType string, inputInfo inputDriver.InputInfo, AddTime int64) *db {
	var isGtid bool
	if inputInfo.GTID != "" {
		isGtid = true
	}
	return &db{
		Name:                    Name,
		ConnectUri:              inputInfo.ConnectUri,
		ConnStatus:              CLOSED,
		ConnErr:                 "",
		LastChannelID:           0,
		ChannelMap:              make(map[int]*Channel),
		TableMap:                make(map[string]*Table),
		IsGtid:                  isGtid,
		Gtid:                    inputInfo.GTID,
		BinlogDumpFileName:      inputInfo.BinlogFileName,
		BinlogDumpPosition:      inputInfo.BinlogPostion,
		MaxBinlogDumpFileName:   inputInfo.MaxFileName,
		MaxBinlogDumpPosition:   inputInfo.MaxPosition,
		ReplicateDoDb:           make(map[string]uint8, 0),
		ServerId:                inputInfo.ServerId,
		killStatus:              0,
		AddTime:                 AddTime,
		LastTransactionTableMap: make(map[string]map[string]bool, 0),
		InputDriverObj:          nil,
		InputType:               InputType,
	}
}

func (db *db) SetServerId(serverId uint32) {
	db.ServerId = serverId
}

func (db *db) SetReplicateDoDb(dbArr []string) bool {
	if db.ConnStatus == CLOSED || db.ConnStatus == STOPPED {
		for i := 0; i < len(dbArr); i++ {
			db.ReplicateDoDb[dbArr[i]] = 1
		}
		return true
	}
	return false
}

func (db *db) AddReplicateDoDb(schemaName, tableName string, doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}
	if tableName == "" {
		return false
	}

	transferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.InputDriverObj != nil {
		db.InputDriverObj.AddReplicateDoDb(schemaName, transferLikeTableReqName)
		logrus.Printf("AddReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ", db.Name, schemaName, tableName, transferLikeTableReqName)
	}
	if _, ok := db.ReplicateDoDb[schemaName]; !ok {
		db.ReplicateDoDb[schemaName] = 1
	}
	return true
}

func (db *db) DelReplicateDoDb(schemaName, tableName string, doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}

	if tableName == "" {
		return false
	}
	transferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.InputDriverObj != nil {
		db.InputDriverObj.DelReplicateDoDb(schemaName, transferLikeTableReqName)
		logrus.Printf("DelReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ", db.Name, schemaName, tableName, transferLikeTableReqName)
	}
	return true
}

func (db *db) TransferLikeTableReq(tableName string) string {
	if tableName == "" {
		return ""
	}

	var reqTableName = tableName
	if tableName != "*" && strings.Index(tableName, "*") > -1 {
		if tableName[0:1] == "*" {
			reqTableName = "(.*)" + tableName[1:]
		}
		// 只要前面不是 （.*）,则自动替换面 ^ 开头，代表前面没数据了
		if strings.Index(reqTableName, "(.*)") != 0 && reqTableName[0:1] != "^" {
			reqTableName = "^" + reqTableName
		}

		var reqTablelen = len(reqTableName)
		if reqTableName[reqTablelen-1:] == "*" {
			reqTableName = reqTableName[0:reqTablelen-1] + "(.*)"
		}
		// 字符串如果不是 (.*) 结尾,则自动替换面 $,代表后面没有数据了
		reqTablelen = len(reqTableName)
		if reqTablelen >= 4 && reqTableName[reqTablelen-4:] != "(.*)" && reqTableName[reqTablelen:] != "$" {
			reqTableName += "$"
		}
	}
	return reqTableName
}

func (db *db) getRightBinlogPosition() (newPosition uint32) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Println(db.Name, " getRightBinlogPosition recover err:", err, " binlogDumpFileName:", db.BinlogDumpFileName, " binlogDumpPosition:", db.BinlogDumpPosition)
			logrus.Println(string(debug.Stack()))
			newPosition = 0
		}
	}()
	err := mysql.CheckBinlogIsRight(db.ConnectUri, db.BinlogDumpFileName, db.BinlogDumpPosition)
	if err == nil {
		return db.BinlogDumpPosition
	}
	logrus.Println(db.Name, " getRightBinlogPosition err:", err, " binlogDumpFileName:", db.BinlogDumpFileName, " binlogDumpPosition:", db.BinlogDumpPosition)
	if strings.Index(err.Error(), "connect: operation timed out") != -1 {
		return newPosition
	}
	newPosition = mysql.GetNearestRightBinlog(db.ConnectUri, db.BinlogDumpFileName, db.BinlogDumpPosition, db.ServerId, db.getReplicateDoDbMap(), nil)
	return newPosition
}

func (db *db) getReplicateDoDbMap() map[string]map[string]uint8 {
	replicateDoDb := make(map[string]map[string]uint8, 0)
	for k, _ := range db.TableMap {
		schemaName, TableName := GetSchemaAndTableBySplit(k)
		if _, ok := replicateDoDb[schemaName]; !ok {
			replicateDoDb[schemaName] = make(map[string]uint8, 0)
		}
		replicateDoDb[schemaName][TableName] = 1
	}
	return replicateDoDb
}

func (db *db) Start() error {
	db.StatusCtx.ctx, db.StatusCtx.cancelFun = context.WithCancel(context.Background())
	db.Lock()
	if db.ConnStatus != CLOSED && db.ConnStatus != STOPPED {
		db.Unlock()
		return errors.New("not be close or stop")
	}
	db.Unlock()
	if db.MaxBinlogDumpFileName == db.BinlogDumpFileName && db.BinlogDumpPosition >= db.MaxBinlogDumpPosition {
		return nil
	}
	if len(db.TableMap) == 0 {
		return errors.New("no table sync config")
	}
	switch db.ConnStatus {
	case CLOSED:
		// 这里加一个加一个方法里再去执行初始化input,是为防止初始化有空指针等异常导致，不释放锁
		// 不放到 InitInputDriver 中去加锁，因为 GetCurrentPosition 中也会去获取Input，不存在的情况下，也会去初始化一次
		func() {
			db.Lock()
			defer db.Unlock()
			db.InitInputDriver()
		}()
		if !db.InputDriverObj.IsSupported(inputDriver.SupportIncre) {
			return fmt.Errorf("DbName: %s Input: %s Incr data is not supported", db.Name, db.InputType)
		}
		db.ConnStatus = STARTING
		go db.InputDriverObj.Start(db.InputStatusChan)
		go db.monitorDump()
		break
	case STOPPED:
		db.ConnStatus = RUNNING
		logrus.Println(db.Name+" monitor:", "running")
		go db.InputDriverObj.Start(db.InputStatusChan)
		break
	default:
		return nil
	}
	go db.CronCalcMinPosition()
	return nil
}

func (db *db) InitInputDriver() {
	inputInfo := inputDriver.InputInfo{
		DbName:         db.Name,
		ConnectUri:     db.ConnectUri,
		GTID:           db.Gtid,
		BinlogFileName: db.BinlogDumpFileName,
		BinlogPostion:  db.BinlogDumpPosition,
		IsGTID:         db.IsGtid,
		ServerId:       db.ServerId,
		MaxFileName:    db.MaxBinlogDumpFileName,
		MaxPosition:    db.MaxBinlogDumpPosition,
	}
	if !db.IsGtid {
		inputInfo.GTID = ""
	}

	db.InputStatusChan = make(chan *inputDriver.PluginStatus, 10)
	db.InputDriverObj = inputDriver.Open(db.InputType, inputInfo)
	db.InputDriverObj.SetCallback(db.Callback)
	for key, _ := range db.TableMap {
		schemaName, TableName := GetSchemaAndTableBySplit(key)
		db.AddReplicateDoDb(schemaName, TableName, false)
	}
	db.InputDriverObj.SetEventID(db.LastEventID)
}

func (db *db) Stop() bool {
	db.Lock()
	defer db.Unlock()

	db.StatusCtx.cancelFun()
	if db.ConnStatus == RUNNING {
		db.InputDriverObj.Stop()
		db.ConnStatus = STOPPED
	}
	return true
}

func (db *db) Close() bool {
	db.Lock()
	defer db.Unlock()
	if db.ConnStatus != STOPPED && db.ConnStatus != STARTING {
		return true
	}
	db.ConnStatus = CLOSING
	db.InputDriverObj.Close()
	return true
}

func (db *db) monitorDump() (r bool) {
	var lastStatus StatusFlag
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	var i uint8 = 0
	for {
		select {
		case inputStatusInfo := <-db.InputStatusChan:
			if inputStatusInfo == nil {
				break
			}
			timer.Reset(3 * time.Second)
			switch inputStatusInfo.Status {
			case inputDriver.RUNNING:
				i = 0
				db.ConnStatus = RUNNING
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGNORMAL,
					DbName: db.Name,
					Body:   " running; last status:" + lastStatus,
				})
			case inputDriver.STARTING:
				db.ConnStatus = STARTING
			case inputDriver.STOPPING:
				db.ConnStatus = STOPPING
			case inputDriver.CLOSING:
				db.ConnStatus = CLOSING
			case inputDriver.STOPPED:
				i = 0
				db.ConnStatus = STOPPED
			case inputDriver.CLOSED:
				db.ConnStatus = CLOSED
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGERROR,
					DbName: db.Name,
					Body:   " closed",
				})
			default:
				db.ConnStatus = DEFAULT
			}
			if inputStatusInfo.Error == nil {
				db.ConnErr = ""
			} else {
				db.ConnErr = inputStatusInfo.Error.Error()
			}
			i++
			if i%3 == 0 || strings.Index(db.ConnErr, "parseEvent err") != -1 {
				i = 0
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGERROR,
					DbName: db.Name,
					Body:   fmt.Sprintf("err:%s; last status:%s", inputStatusInfo.Error, lastStatus),
				})
			}

			logrus.Println(db.Name+" monitor:", db.ConnStatus, db.ConnErr)
			lastStatus = db.ConnStatus

			break
		case <-timer.C:
			timer.Reset(3 * time.Second)
			db.saveBinlog()
			break
		}
	}
	return true
}

func (db *db) saveBinlog() {
	p := db.InputDriverObj.GetLastPosition()
	if p == nil {
		return
	}

	//保存位点,这个位点在重启 配置文件恢复的时候
	db.Lock()
	defer db.Unlock()
	db.BinlogDumpFileName, db.BinlogDumpPosition, db.BinlogDumpTimestamp, db.Gtid, db.LastEventID = p.BinlogFileName, p.BinlogPostion, p.Timestamp, p.GTID, p.EventID
	if db.DBBinlogKey == nil {
		db.DBBinlogKey = getBinlogKey(db)
	}
	var BinlogFileNum int
	if p.BinlogFileName != "" {
		index := strings.IndexAny(p.BinlogFileName, ".")
		BinlogFileNum, _ = strconv.Atoi(p.BinlogFileName[index+1:])
	}

	var lastParseBinlog = &PositionStruct{
		BinlogFileNum:  BinlogFileNum,
		BinlogPosition: p.BinlogPostion,
		GTID:           p.GTID,
		Timestamp:      p.Timestamp,
		EventID:        p.EventID,
	}
	saveBinlogPosition(db.DBBinlogKey, lastParseBinlog)
}

func (db *db) IgnoreTableToMap(IgnoreTable string) map[string]bool {
	if IgnoreTable == "" {
		return nil
	}
	m := make(map[string]bool)
	for _, tableName := range strings.Split(IgnoreTable, ",") {
		if tableName == "" {
			continue
		}
		m[tableName] = true
	}
	return m
}

func (db *db) AddTable(schemaName, tableName, ignoreTable, doTable string, channelKey, lastToServerId int) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.TableMap[key]; !ok {
		db.TableMap[key] = &Table{
			Key:            key,
			Name:           tableName,
			ChannelKey:     channelKey,
			ToServerList:   make([]*ToServer, 0),
			LastToServerID: lastToServerId,
			LikeTableList:  make([]*Table, 0),
			DoTable:        doTable,
			DoTableMap:     db.IgnoreTableToMap(doTable),
			IgnoreTable:    ignoreTable,
			IgnoreTableMap: db.IgnoreTableToMap(ignoreTable),
		}
		db.addLikeTable(db.TableMap[key], schemaName, tableName)
		logrus.Println("addTable", db.Name, schemaName, tableName, db.ChannelMap[channelKey].Name, " IgnoreTable:", ignoreTable, "DoTable:", doTable)
		count.SetTable(db.Name, key)
	}
	return true
}

// UpdateTable 修改模糊匹配的表规则 需要过滤哪些表不进行匹配
func (db *db) UpdateTable(schemaName, tableName, ignoreTable, doTable string) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.TableMap[key]; !ok {
		logrus.Println("UpdateTable ", db.Name, schemaName, tableName, " not exsit ")
		return false
	}
	db.TableMap[key].DoTable = doTable
	db.TableMap[key].DoTableMap = db.IgnoreTableToMap(doTable)
	db.TableMap[key].IgnoreTable = ignoreTable
	db.TableMap[key].IgnoreTableMap = db.IgnoreTableToMap(ignoreTable)
	logrus.Println("UpdateTable", db.Name, schemaName, tableName, "IgnoreTable:", ignoreTable, "DoTable:", doTable)
	return true
}

func (db *db) addLikeTable(t *Table, schemaName, tableName string) {
	if tableName == "*" || strings.Index(tableName, "*") == -1 {
		return
	}

	key := GetSchemaAndTableJoin(schemaName, tableName)
	reqTableName := db.TransferLikeTableReq(tableName)
	reqTagAll, err := regexp.Compile(reqTableName)
	if err != nil {
		logrus.Println(db.Name, " addLikeTable :", key, "reqTableName:", reqTableName, " reqTagAll err:", err)
		return
	}
	for k, v := range db.TableMap {
		if strings.Index(k, "*") >= 0 {
			continue
		}
		schemaName0, TableName0 := GetSchemaAndTableBySplit(k)
		if schemaName0 != schemaName {
			continue
		}
		// 假如匹配的表
		if reqTagAll.FindString(TableName0) != "" {
			v.LikeTableList = append(v.LikeTableList, t)
		}
	}
}

func (db *db) GetTable(schemaName, tableName string) *Table {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	return db.GetTableByKey(key)
}

func (db *db) GetTableByKey(key string) *Table {
	db.RLock()
	if _, ok := db.TableMap[key]; !ok {
		db.RUnlock()
		//这里判断 > 0 ，假如 == 0 说明是所有表了了，如果是 == * 的情况下，是有 所有表的逻辑，已经存到map中了
		db.Lock()
		defer db.Unlock()
		schemaName, TableName := GetSchemaAndTableBySplit(key)
		key0 := GetSchemaAndTableJoin(schemaName, "*")
		for k, v := range db.TableMap {
			if k == key0 {
				continue
			}
			//库名是 * 或者 table 里没有 * 的，都不匹配
			if strings.Index(k, "*") == -1 {
				continue
			}
			if v.RegexpErr {
				continue
			}
			schemaName0, TableName0 := GetSchemaAndTableBySplit(k)
			if schemaName0 != schemaName {
				continue
			}
			reqTagAll, err := regexp.Compile(db.TransferLikeTableReq(TableName0))
			if err != nil {
				v.RegexpErr = true
				logrus.Println(db.Name, " GetTable :", k, "TransferLikeTableReq:", db.TransferLikeTableReq(TableName0), "reqTagAll err:", err)
				continue
			}
			if reqTagAll.FindString(TableName) != "" {
				if _, ok := db.TableMap[key]; !ok {
					db.TableMap[key] = &Table{
						Key:           key,
						ChannelKey:    v.ChannelKey,
						ToServerList:  make([]*ToServer, 0),
						LikeTableList: make([]*Table, 0),
					}
					count.SetTable(db.Name, key)
				}
				db.TableMap[key].LikeTableList = append(db.TableMap[key].LikeTableList, v)
			}
		}
		if _, ok := db.TableMap[key]; ok {
			return db.TableMap[key]
		}
		return nil
	} else {
		defer db.RUnlock()
		return db.TableMap[key]
	}
}

func (db *db) GetTableSelf(schemaName, tableName string) *Table {
	return db.GetTable(schemaName, tableName)
}

func (db *db) GetTables() map[string]*Table {
	return db.TableMap
}

func (db *db) GetTableByChannelKey(schemaName string, channelKey int) (TableMap map[string]*Table) {
	TableMap = make(map[string]*Table)
	for k, v := range db.TableMap {
		if v.ChannelKey == channelKey && len(v.ToServerList) > 0 {
			TableMap[k] = v
		}
	}
	return
}

// DelTable 删除表和通道的绑定关系
// 假如存在表和同步关系，则需要将这个表从 binlog 解析中也去删除掉
func (db *db) DelTable(schemaName string, tableName string) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()

	if _, ok := db.TableMap[key]; !ok {
		return true
	}

	t := db.TableMap[key]
	toServerLen := len(t.ToServerList)
	for _, toServerInfo := range t.ToServerList {
		if toServerInfo.Status == RUNNING {
			toServerInfo.Status = DELING
		}
	}
	delete(db.TableMap, key)

	if tableName != "*" && strings.Index(tableName, "*") >= 0 {
		for _, v := range db.TableMap {
			for index, v0 := range v.LikeTableList {
				if v0 == t {
					if index == len(v.LikeTableList)-1 {
						v.LikeTableList = v.LikeTableList[:len(v.LikeTableList)-1]
					} else {
						v.LikeTableList = append(v.LikeTableList[:index], v.LikeTableList[index+1:]...)
					}
					break
				}
			}
		}
	}
	count.DelTable(db.Name, key)
	logrus.Println("delTable", db.Name, schemaName, tableName)
	if db.InputDriverObj != nil && toServerLen > 0 {
		db.DelReplicateDoDb(schemaName, tableName, false)
	}
	return true
}

func (db *db) AddChannel(name string, maxThreadNum int) (*Channel, int) {
	db.Lock()
	defer db.Unlock()

	db.LastChannelID++
	channelId := db.LastChannelID
	if _, ok := db.ChannelMap[channelId]; ok {
		return db.ChannelMap[channelId], channelId
	}

	c := NewChannel(maxThreadNum, name, db)
	db.ChannelMap[channelId] = c
	ch := count.SetChannel(db.Name, name)
	db.ChannelMap[channelId].SetFlowCountChan(ch)

	logrus.Println("addChannel", db.Name, name, "maxThreadNum:", maxThreadNum)
	return db.ChannelMap[channelId], channelId
}

func (db *db) ListChannel() map[int]*Channel {
	db.Lock()
	defer db.Unlock()
	return db.ChannelMap
}

func (db *db) GetChannel(channelId int) *Channel {
	if _, ok := db.ChannelMap[channelId]; !ok {
		return nil
	}
	return db.ChannelMap[channelId]
}

func (db *db) GetCurrentPosition() (*inputDriver.PluginPosition, error) {
	inputDriverObj := db.GetInputDriverObj()
	if inputDriverObj == nil {
		return nil, nil
	}
	return inputDriverObj.GetCurrentPosition()
}

func (db *db) GetInputDriverObj() inputDriver.Driver {
	db.Lock()
	defer db.Unlock()
	if db.InputDriverObj == nil {
		db.InitInputDriver()
	}
	return db.InputDriverObj
}

// AddTableToServer 新增表的同步配置
// 假如是第一次添加的表同步配置，则需要通知 binlog 解析库，解析当前表的binlog
func (db *db) AddTableToServer(schemaName string, tableName string, toServer *ToServer) (bool, int) {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.TableMap[key]; !ok {
		return false, 0
	}
	if toServer.ToServerID <= 0 {
		db.TableMap[key].LastToServerID += 1
		toServer.ToServerID = db.TableMap[key].LastToServerID
	}
	if toServer.PluginName == "" {
		ToServerInfo := pluginStorage.GetToServerInfo(toServer.ToServerKey)
		if ToServerInfo != nil {
			toServer.PluginName = ToServerInfo.PluginName
		}
	}

	var Binlog0 = &PositionStruct{}
	if toServer.LastQueueBinlog == nil {
		BinlogPostion, err := getBinlogPosition(getBinlogKey(db))
		if err == nil {
			// 这里手工复制的原因是要防止 数据出错
			Binlog0 = &PositionStruct{
				BinlogFileNum:  BinlogPostion.BinlogFileNum,
				BinlogPosition: BinlogPostion.BinlogPosition,
				GTID:           BinlogPostion.GTID,
				Timestamp:      BinlogPostion.Timestamp,
				EventID:        BinlogPostion.EventID,
			}
		}
		toServer.LastQueueBinlog = Binlog0
		toServer.LastSuccessBinlog = Binlog0
	}
	if toServer.LastSuccessBinlog == nil {
		toServer.LastSuccessBinlog = Binlog0
	}

	toServer.Key = key
	toServer.QueueMsgCount = 0
	toServer.StatusChan = make(chan bool, 1)
	db.TableMap[key].ToServerList = append(db.TableMap[key].ToServerList, toServer)

	// 在添加第一个同步的时候，通知 binlog 解析，需要同步这个表
	if len(db.TableMap[key].ToServerList) == 1 && db.InputDriverObj != nil {
		db.AddReplicateDoDb(schemaName, tableName, false)
	}
	logrus.Println("AddTableToServer", db.Name, schemaName, tableName, toServer)
	return true, toServer.ToServerID
}

// DelTableToServer  删除表的同步配置
// 假如当前表没有其他同步配置了，则需要从 binlog 解析中删除掉，不再需要 解析这个表的数据
func (db *db) DelTableToServer(schemaName, tableName string, toServerID int) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.TableMap[key]; !ok {
		return false
	}
	var index = -1
	for index1, toServerInfo2 := range db.TableMap[key].ToServerList {
		if toServerInfo2.ToServerID == toServerID {
			index = index1
			break
		}
	}

	if index == -1 {
		return true
	}

	toServerInfo := db.TableMap[key].ToServerList[index]
	toServerPositionBinlogKey := getToServerBinlogKey(db, toServerInfo)
	if index == len(db.TableMap[key].ToServerList)-1 {
		db.TableMap[key].ToServerList = db.TableMap[key].ToServerList[:len(db.TableMap[key].ToServerList)-1]
	} else {
		db.TableMap[key].ToServerList = append(db.TableMap[key].ToServerList[:index], db.TableMap[key].ToServerList[index+1:]...)
	}

	if toServerInfo.Status == RUNNING || toServerInfo.Status == STOPPING {
		toServerInfo.Status = DELING
	} else {
		if toServerInfo.Status != DELING {
			delBinlogPosition(toServerPositionBinlogKey)
		}
	}
	// 当前这个表都没有同步配置了，则通知 binlog 解析，不再需要解析这个表的数据了
	if len(db.TableMap[key].ToServerList) == 0 && db.InputDriverObj != nil {
		db.DelReplicateDoDb(schemaName, tableName, false)
	}
	logrus.Println("DelTableToServer", db.Name, schemaName, tableName, "toServerInfo:", toServerInfo)
	filequeue.Delete(GetFileQueue(db.Name, schemaName, tableName, fmt.Sprint(toServerID)))
	return true
}
