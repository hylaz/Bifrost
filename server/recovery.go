package server

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/filequeue"
	"github.com/sirupsen/logrus"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

type dbSaveInfo struct {
	Name                  string                  `json:"Name"`
	InputType             string                  `json:"InputType"`
	ConnectUri            string                  `json:"ConnectUri"`
	ConnStatus            StatusFlag              `json:"ConnStatus"`
	ConnErr               string                  `json:"ConnErr"`
	ChannelMap            map[int]channelSaveInfo `json:"ChannelMap"`
	LastChannelID         int                     `json:"LastChannelID"`
	TableMap              map[string]*Table       `json:"TableMap"`
	IsGtid                bool                    `json:"IsGtid"`
	LastEventID           uint64                  `json:"LastEventID"`
	Gtid                  string                  `json:"Gtid"`
	BinlogDumpFileName    string                  `json:"BinlogDumpFileName"`
	BinlogDumpPosition    uint32                  `json:"BinlogDumpPosition"`
	BinlogDumpTimestamp   uint32                  `json:"BinlogDumpTimestamp"`
	MaxBinlogDumpFileName string                  `json:"MaxBinlogDumpFileName"`
	MaxinlogDumpPosition  uint32                  `json:"MaxinlogDumpPosition"`
	ReplicateDoDb         map[string]uint8        `json:"ReplicateDoDb"`
	ServerId              uint32                  `json:"ServerId"`
	AddTime               int64                   `json:"AddTime"`
}

type channelSaveInfo struct {
	Name             string
	MaxThreadNum     int // 消费通道的最大线程数
	CurrentThreadNum int
	Status           StatusFlag //stop ,starting,running,wait
}

func CompareBinlogPositionAndReturnGreater(binlog1, binlog2 *PositionStruct) *PositionStruct {
	if binlog1 == nil {
		return binlog2
	}
	if binlog2 == nil {
		return binlog1
	}
	if binlog1.EventID > binlog2.EventID {
		return binlog1
	}
	if binlog1.EventID < binlog2.EventID {
		return binlog2
	}

	if binlog1.Timestamp > binlog2.Timestamp {
		return binlog1
	}
	if binlog1.Timestamp < binlog2.Timestamp {
		return binlog2
	}

	if binlog1.BinlogFileNum > binlog2.BinlogFileNum {
		return binlog1
	} else if binlog1.BinlogFileNum == binlog2.BinlogFileNum {
		if binlog1.BinlogPosition >= binlog2.BinlogPosition {
			return binlog1
		} else {
			return binlog2
		}
	} else {
		return binlog2
	}
}

func CompareBinlogPositionAndReturnLess(binlog1, binlog2 *PositionStruct) *PositionStruct {
	if binlog1 == nil {
		return binlog2
	}
	if binlog2 == nil {
		return binlog1
	}
	if binlog1.EventID > binlog2.EventID {
		return binlog2
	}
	if binlog1.EventID < binlog2.EventID {
		return binlog1
	}

	if binlog1.Timestamp > binlog2.Timestamp {
		return binlog2
	}
	if binlog1.Timestamp < binlog2.Timestamp {
		return binlog1
	}
	if binlog1.BinlogFileNum > binlog2.BinlogFileNum && binlog2.BinlogFileNum > 0 {
		return binlog2
	} else if binlog1.BinlogFileNum == binlog2.BinlogFileNum {
		if binlog1.BinlogPosition >= binlog2.BinlogPosition {
			return binlog2
		} else {
			return binlog1
		}
	} else {
		if binlog1.BinlogFileNum > 0 {
			return binlog1
		} else {
			return binlog2
		}
	}
}

func Recovery(content *json.RawMessage, isStop bool) {
	var data map[string]dbSaveInfo
	err := json.Unmarshal(*content, &data)
	if err != nil {
		logrus.Info("recorery db content errors;", err)
		os.Exit(1)
		return
	}

	recoveryData(data, isStop)
}

func recoveryData(data map[string]dbSaveInfo, isStop bool) {
	for name, dbInfo := range data {
		channelIdMap := make(map[int]int)
		inputInfo := inputDriver.InputInfo{
			IsGTID:         dbInfo.IsGtid,
			ConnectUri:     dbInfo.ConnectUri,
			GTID:           dbInfo.Gtid,
			BinlogFileName: dbInfo.BinlogDumpFileName,
			BinlogPostion:  dbInfo.BinlogDumpPosition,
			ServerId:       dbInfo.ServerId,
			MaxFileName:    dbInfo.MaxBinlogDumpFileName,
			MaxPosition:    dbInfo.MaxinlogDumpPosition,
		}
		if dbInfo.InputType == "" {
			dbInfo.InputType = "mysql"
		}
		db := AddNewDb(name, dbInfo.InputType, inputInfo, dbInfo.AddTime)
		if db == nil {
			logrus.Println("recovry data error2,data:", dbInfo)
			os.Exit(1)
		}
		m := make([]string, 0)
		for schemaName, _ := range dbInfo.ReplicateDoDb {
			m = append(m, schemaName)
		}
		//db.SetReplicateDoDb(m)
		for oldChannelId, cInfo := range dbInfo.ChannelMap {
			ch, ChannelID := db.AddChannel(cInfo.Name, cInfo.MaxThreadNum)
			ch.Status = CLOSED
			channelIdMap[oldChannelId] = ChannelID
			// 只要不是 close 状态，就启动
			if cInfo.Status != CLOSED {
				if dbInfo.MaxBinlogDumpFileName == "" || (dbInfo.BinlogDumpFileName != dbInfo.MaxBinlogDumpFileName && dbInfo.BinlogDumpPosition != dbInfo.MaxinlogDumpPosition) {
					ch.Start()
					continue
				}
			}
			//db close,channel must be close
			ch.Close()
		}

		// 二进制文件格式是xxx.000001 ,后面数字是6位数，不够前面补0
		index := strings.IndexAny(dbInfo.BinlogDumpFileName, ".")
		binlogPrefix := dbInfo.BinlogDumpFileName[0:index]

		var PerformanceTestingFileName string = ""
		var PerformanceTestingFileNum int = 0
		var PerformanceTestingPosition uint32 = 0
		var PerformanceTestingGTID string = ""
		//假如在性能测试的配置文件中找到这个 找这个DbName，则直接加载配置文件中的位点
		//并且只有配置合法，才可以通过

		func() {
			if PerformanceTestingParam := config.GetConfigVal("PerformanceTesting", dbInfo.Name); PerformanceTestingParam != "" {
				logrus.Println("PerformanceTesting", dbInfo.Name, "PerformanceTestingParam:", PerformanceTestingParam)
				t := strings.Split(PerformanceTestingParam, ",")
				var err error
				defer func() {
					if err != nil {
						logrus.Println("PerformanceTesting", dbInfo.Name, err)
					}
				}()
				if len(t) < 2 {
					err = fmt.Errorf("PerformanceTestingParam:%s split error", PerformanceTestingParam)
					return
				}

				index := strings.IndexAny(t[0], ".")
				binlogPrefix2 := t[0][0:index]
				PerformanceTestingFileNum, err = strconv.Atoi(t[0][index+1:])
				if err != nil {
					return
				}
				//配置文件 和  原有DbName里设置的binlog 前缀必须一致
				if binlogPrefix2 != binlogPrefix {
					err = fmt.Errorf("binlog prefix:%s != %s", binlogPrefix2, binlogPrefix)
					return
				}
				//位点也必须是数字
				var position int
				position, err = strconv.Atoi(t[1])
				if err != nil {
					return
				}
				if position < 4 {
					err = fmt.Errorf("position:%d < 4", position)
					return
				}
				PerformanceTestingFileName = t[0]
				PerformanceTestingPosition = uint32(position)
				if len(t) > 2 {
					for i := 2; i < len(t); i++ {
						if PerformanceTestingGTID == "" {
							PerformanceTestingGTID = t[i]
						} else {
							PerformanceTestingGTID += "," + t[i]
						}
					}
				}
			}
		}()

		var LastBinlog = &PositionStruct{
			BinlogFileNum:  0,
			BinlogPosition: 0,
			GTID:           "",
			Timestamp:      0,
			EventID:        0,
		}
		var lastAllToServerNoraml = true
		if len(dbInfo.TableMap) > 0 {
			// 优先 非模糊批配的同步配置，因为有些 模糊匹配的表，关联了 模糊匹配的虚拟表，如果 先 遍历 模糊匹配的虚拟表，就会出现丢掉一部分 非模拟匹配的表同步配置
			ToServerList1 := make([]*Table, 0)
			ToServerList2 := make([]*Table, 0)
			for tKey, tInfo := range dbInfo.TableMap {
				tInfo.Key = tKey
				if strings.Index(tKey, "*") == -1 {
					ToServerList1 = append(ToServerList1, tInfo)
				} else {
					ToServerList2 = append(ToServerList2, tInfo)
				}
			}
			ToServerList1 = append(ToServerList1, ToServerList2...)
			for _, tInfo := range ToServerList1 {
				if tInfo.ChannelKey <= 0 || len(tInfo.ToServerList) == 0 {
					continue
				}

				schemaName, tableName := GetSchemaAndTableBySplit(tInfo.Key)
				db.AddTable(schemaName, tableName, tInfo.IgnoreTable, tInfo.DoTable, channelIdMap[tInfo.ChannelKey], tInfo.LastToServerID)
				for _, toServer := range tInfo.ToServerList {
					toServerBinlogPositionFromDB, _ := getBinlogPosition(getToServerBinlogKey(db, toServer))
					var toServerBinlog *PositionStruct
					if toServer.LastSuccessBinlog == nil {
						toServerBinlog = &PositionStruct{
							BinlogFileNum:  toServer.BinlogFileNum,
							BinlogPosition: toServer.BinlogPosition,
							GTID:           "",
							Timestamp:      0,
							EventID:        0,
						}
					} else {
						toServerBinlog = toServer.LastSuccessBinlog
					}
					if toServerBinlog == nil {
						toServerBinlog = &PositionStruct{}
					}

					// 和 leveldb 中数据做对比，取大值, 因为leveldb中的数据是定时刷盘的，而配置中的 位点信息，是在正常退出进程的时候,才会刷到磁盘的，在异常断电等情况下，位点就错了
					// 下同
					if toServerBinlogPositionFromDB != nil {
						toServerBinlog = CompareBinlogPositionAndReturnGreater(toServerBinlog, toServerBinlogPositionFromDB)
					}
					toServerLastBinlogPositionFromDB, _ := getBinlogPosition(getToServerLastBinlogKey(db, toServer))

					// 这里为了兼容 1.6.0及之前版本的 配置
					var toServerLastQueueBinlog *PositionStruct
					if toServer.LastQueueBinlog == nil {
						toServerLastQueueBinlog = &PositionStruct{
							BinlogFileNum:  toServer.LastBinlogFileNum,
							BinlogPosition: toServer.LastBinlogPosition,
							GTID:           "",
							Timestamp:      0,
							EventID:        0,
						}
					} else {
						toServerLastQueueBinlog = toServer.LastQueueBinlog
					}
					if toServerLastQueueBinlog == nil {
						toServerLastQueueBinlog = &PositionStruct{}
					}
					if toServerLastBinlogPositionFromDB != nil {
						toServerLastQueueBinlog = CompareBinlogPositionAndReturnGreater(toServerLastQueueBinlog, toServerLastBinlogPositionFromDB)
					}

					//假如是性能测试，能强制修改为指定位点

					if PerformanceTestingFileName != "" {
						toServerBinlog.BinlogFileNum = PerformanceTestingFileNum
						toServerBinlog.BinlogPosition = PerformanceTestingPosition
						toServerLastQueueBinlog.BinlogFileNum = PerformanceTestingFileNum
						toServerLastQueueBinlog.BinlogPosition = PerformanceTestingPosition
						toServerBinlog.GTID = PerformanceTestingGTID
						toServerBinlog.EventID = 0
					}

					// 假如没有开启文件队列,则不管什么情况，都不启用文件队列
					if config.FileQueueUsable == false {
						toServer.FileQueueStatus = false
					}
					// 1.6 之前版本, 还是用stop ,close ，而不是stopped,closed
					if toServer.Status == "stop" {
						toServer.Status = STOPPED
					}
					if toServer.Status == "close" {
						toServer.Status = CLOSED
					}
					var status StatusFlag
					switch toServer.Status {
					case STOPPING, STOPPED:
						status = STOPPED
						break
					case DELING, DELED:
						continue
					default:
						break
					}
					toServerObj := &ToServer{
						ToServerID:        toServer.ToServerID,
						MustBeSuccess:     toServer.MustBeSuccess,
						FilterQuery:       toServer.FilterQuery,
						FilterUpdate:      toServer.FilterUpdate,
						ToServerKey:       toServer.ToServerKey,
						PluginName:        toServer.PluginName,
						FieldList:         toServer.FieldList,
						BinlogFileNum:     toServerBinlog.BinlogFileNum,
						BinlogPosition:    toServerBinlog.BinlogPosition,
						LastSuccessBinlog: toServerBinlog,
						LastQueueBinlog:   toServerLastQueueBinlog,
						PluginParam:       toServer.PluginParam,
						FileQueueStatus:   toServer.FileQueueStatus,
						Status:            status,
					}
					if toServerObj.FileQueueStatus {
						var lastDataEvent *pluginDriver.PluginDataType
						var err error
						func() {
							if e := recover(); e != nil {
								logrus.Printf("dbName:%s ;SchemaName:%s ; TableName:%s ; ReadLastFromFileQueue recovry:%s ; debug:%s", db.Name, schemaName, tableName, fmt.Sprint(e), string(debug.Stack()))
								return
							}
							lastDataEvent, err = toServerObj.InitFileQueue(db.Name, schemaName, tableName).ReadLastFromFileQueue()
						}()
						if err != nil {
							logrus.Fatal(fmt.Sprintf("dbName:%s ;SchemaName:%s ; TableName:%s ; ReadLastFromFileQueue Error:%s", db.Name, schemaName, tableName, err.Error()))
						}
						// 假如没有找到数据，或者文件队列里的最后一条数据，位点 对不上 ToServer里保存的数据，则认为数据是有异常的，则需要将 FileQueueStatus 修改为  false,清空文件队列数据
						// 假如文件队列里最后一条数据和当前同步记录的进入 这个同步最后一个位点数据 相等，则不进行位点计算，随便其他 同步位点怎么来
						if lastDataEvent == nil || lastDataEvent.BinlogFileNum != toServerObj.LastQueueBinlog.BinlogFileNum || lastDataEvent.BinlogPosition != toServerObj.LastQueueBinlog.BinlogPosition {
							toServerObj.FileQueueStatus = false
						}
					}
					if toServerObj.FileQueueStatus == false {
						//FileQueueStatus == false 这里强制将将文件队列数据清除，因为有可能会有脏数据
						filequeue.Delete(GetFileQueue(db.Name, schemaName, tableName, fmt.Sprint(toServerObj.ToServerID)))
					}
					db.AddTableToServer(schemaName, tableName, toServerObj)
					logrus.Printf("dbname:%s,schemaName:%s,tableName:%s ToServerKey:%s,ToServerID:%d,BinlogFileNum:%d,BinlogPosition:%d", db.Name, schemaName, tableName, toServer.ToServerKey, toServer.ToServerID, toServer.BinlogFileNum, toServer.BinlogPosition)

					// 假如当前同步配置 最后输入的 位点 等于 最后成功的位点为0,则认为这个同步，压根就没有数据进来过,位点是没有问题的
					if toServer.LastBinlogFileNum == 0 {
						continue
					}
					//假如当前同步配置 最后输入的 位点 等于 最后成功的位点，说明当前这个 同步配置的位点是没有问题的
					if toServerBinlog.BinlogFileNum > 0 && toServerBinlog.BinlogFileNum == toServerLastQueueBinlog.BinlogFileNum && toServerBinlog.BinlogPosition == toServerLastQueueBinlog.BinlogPosition {
						if lastAllToServerNoraml {
							//假如所有表都还是正常同步的情况下，LastBinlog 取大值
							LastBinlog0 := CompareBinlogPositionAndReturnGreater(toServerBinlog, LastBinlog)
							if LastBinlog0 == toServerBinlog {
								logrus.Println("recovery binlog change2:", dbInfo.Name, " old", " BinlogFileNum:", LastBinlog.BinlogFileNum, " BinlogPosition:", LastBinlog.BinlogPosition, " GTID:", LastBinlog.GTID, " new BinlogFileNum:", toServerBinlog.BinlogFileNum, " BinlogPosition:", toServerBinlog.BinlogPosition, "GTID:", toServerBinlog.GTID)
								LastBinlog = LastBinlog0
							}
						}
						// 因为判定当前这个同步的位点 是没有问题的，所以就不需要再参与最小位点计算，所以这里不需要和其他位点进行对比，取小值
						continue
					} else {
						lastAllToServerNoraml = false
					}
					if lastAllToServerNoraml == false {
						//假如同步异常的情况下，取小值
						//假如 BinlogFileNum = 0 的情况下，则用当前最小的同步位点
						if LastBinlog.BinlogFileNum == 0 {
							LastBinlog.BinlogFileNum = toServerBinlog.BinlogFileNum
							LastBinlog.BinlogPosition = toServerBinlog.BinlogPosition
							logrus.Println("recovery binlog change3:", dbInfo.Name, " old", " BinlogFileNum:", 0, " BinlogPosition:", 0, " GTID:", "", " new BinlogFileNum:", toServerBinlog.BinlogFileNum, " ", toServerBinlog.BinlogPosition, "GTID:", toServerBinlog.GTID)

						} else {
							LastBinlog0 := CompareBinlogPositionAndReturnLess(LastBinlog, toServerBinlog)
							if LastBinlog0 == toServerBinlog {
								logrus.Println("recovery binlog change1:", dbInfo.Name, " old", " BinlogFileNum:", LastBinlog.BinlogFileNum, " BinlogPosition:", LastBinlog.BinlogPosition, " GTID:", LastBinlog.GTID, " new ", "BinlogFileNum:", toServerBinlog.BinlogFileNum, " BinlogPosition:", toServerBinlog.BinlogPosition, "GTID:", toServerBinlog.GTID)
								LastBinlog = LastBinlog0
							}
						}
					}
					if toServer.LastBinlogFileNum == 0 && toServer.BinlogFileNum > 0 {
						saveBinlogPosition(getToServerLastBinlogKey(db, toServer), toServerBinlog)
					}
				}
			}
		}

		// 数据源自行保存的位点
		// 拿镜像数据里的 位点和level中存储 db 位点对比，取更大的值
		// 因为镜像数据只有配置更改了才会修改，但是 level中的数据是只要有数据同步 及 每5秒强制刷一次
		LastDBBinlogFileNum, _ := strconv.Atoi(dbInfo.BinlogDumpFileName[index+1:])
		DBBinlogKey := getBinlogKey(db)
		DBLastBinlogPositionFromDB, _ := getBinlogPosition(DBBinlogKey)
		var DBBinlog = &PositionStruct{
			BinlogFileNum:  LastDBBinlogFileNum,
			BinlogPosition: db.BinlogDumpPosition,
			GTID:           db.Gtid,
			Timestamp:      db.BinlogDumpTimestamp,
			EventID:        db.LastEventID,
		}
		if DBLastBinlogPositionFromDB != nil {
			//假如key val存储中DB 的位点值存在 取大值
			DBBinlog0 := CompareBinlogPositionAndReturnGreater(DBBinlog, DBLastBinlogPositionFromDB)
			if DBBinlog0 == DBLastBinlogPositionFromDB {
				logrus.Println("recovery DBBinlog change:", dbInfo.Name, " old BinlogFileNum:", LastDBBinlogFileNum, " BinlogPosition:", db.BinlogDumpPosition, " GTID:", db.Gtid, " new BinlogFileNum:", DBLastBinlogPositionFromDB.BinlogFileNum, " BinlogPosition:", DBLastBinlogPositionFromDB.BinlogPosition, " GITD:", DBLastBinlogPositionFromDB.GTID)
				DBBinlog = DBBinlog0
			}
		}

		//假如所有表数据同步都是正常的，则取 db 里的位点配置，否则取 同步表里最小位点
		//假如有一个表的数据同步位点不正常,则取不正常位点的最小值,否则取和当前db最后保存的位 及表位点的最大值
		if lastAllToServerNoraml == false {
			// 假如表同步的计算出来的最小位点都是有问题的, 则直接用 数据源记录的位点
			if LastBinlog == nil || LastBinlog.BinlogFileNum <= 0 {
				LastBinlog = DBBinlog
			}
		} else {
			//这里为什么要取大值,是因为位点是定时刷盘的,有可能在哪些特殊情况下,表位点成功了,db位点没保存成功
			LastBinlog0 := CompareBinlogPositionAndReturnGreater(DBBinlog, LastBinlog)
			if LastBinlog0 == DBBinlog {
				logrus.Println("recovery binlog change5:", dbInfo.Name, " old BinlogFileNum:", LastBinlog.BinlogFileNum, " BinlogPosition:", LastBinlog.BinlogPosition, " GTID:", LastBinlog.GTID, " new BinlogFileNum:", DBBinlog.BinlogFileNum, " BinlogPosition:", DBBinlog.BinlogPosition, " GITD:", DBBinlog.GTID)
				LastBinlog = LastBinlog0
			}
		}
		db.BinlogDumpFileName = binlogPrefix + "." + fmt.Sprintf("%06d", LastBinlog.BinlogFileNum)
		if LastBinlog.GTID != "" {
			db.Gtid = LastBinlog.GTID
		}
		db.BinlogDumpPosition = LastBinlog.BinlogPosition
		db.LastEventID = LastBinlog.EventID

		//如果是性能测试配置，强制修改位点
		if PerformanceTestingFileName != "" {
			db.BinlogDumpFileName = PerformanceTestingFileName
			db.BinlogDumpPosition = PerformanceTestingPosition
			db.Gtid = PerformanceTestingGTID
		}

		if dbInfo.ConnStatus == CLOSING {
			dbInfo.ConnStatus = CLOSED
		}
		if dbInfo.ConnStatus != CLOSED && dbInfo.ConnStatus != STOPPED && dbInfo.ConnStatus != STOPPING && !isStop {
			// 只要其中一个不相等，就说明位点是不一样
			if dbInfo.BinlogDumpFileName != dbInfo.MaxBinlogDumpFileName || dbInfo.BinlogDumpPosition != dbInfo.MaxinlogDumpPosition {
				go db.Start()
			}
		}
	}

	//启动同步的消费线程
	for _, db := range DbList {
		for tableKey, t := range db.TableMap {
			for _, toServer := range t.ToServerList {
				if toServer.FileQueueStatus == false {
					continue
				}
				SchemaName, TableName := GetSchemaAndTableBySplit(tableKey)
				func() {
					toServer.Lock()
					defer toServer.Unlock()
					toServer.ToServerChan = &ToServerChan{
						To: make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
					}
					go toServer.consumeToServer(db, SchemaName, TableName)
				}()
			}
		}
	}

}

func StopAllChannel() {
	DbLock.Lock()
	defer DbLock.Unlock()
	for _, db := range DbList {
		db.killStatus = 1
		if db.InputStatusChan != nil {
			close(db.InputStatusChan)
		}

		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()

			if db.InputDriverObj != nil {
				db.InputDriverObj.Kill()
			}
		}()
	}
}

func SaveDBInfoToFileData() any {
	DbLock.Lock()
	defer DbLock.Unlock()

	data := make(map[string]dbSaveInfo)

	for k, db := range DbList {
		db.Lock()
		defer db.Unlock()
		// 假如数据源是mysql,没有开启gtid同步功能，但是又有gtid信息的情况下，但是后端又能获取到gtid信息，启退的时候,还是会获取到gtid进行保留
		// db.isGtid 是在启动的时候判断是否有gtid
		// 所以在退出保存配置的时候，也应该判断在启动数据源的时候，是否有真正gtid信息，否则直接为空，防止中间被自动，导致重启后使用不了

		var gtid string
		if db.IsGtid {
			gtid = db.Gtid
		}

		data[k] = dbSaveInfo{
			Name:                  db.Name,
			InputType:             db.InputType,
			ConnectUri:            db.ConnectUri,
			ConnStatus:            db.ConnStatus,
			LastChannelID:         db.LastChannelID,
			BinlogDumpFileName:    db.BinlogDumpFileName,
			BinlogDumpPosition:    db.BinlogDumpPosition,
			IsGtid:                db.IsGtid,
			Gtid:                  gtid,
			LastEventID:           db.LastEventID,
			BinlogDumpTimestamp:   db.BinlogDumpTimestamp,
			MaxBinlogDumpFileName: db.MaxBinlogDumpFileName,
			MaxinlogDumpPosition:  db.MaxBinlogDumpPosition,
			ReplicateDoDb:         db.ReplicateDoDb,
			ServerId:              db.ServerId,
			ChannelMap:            make(map[int]channelSaveInfo, 0),
			TableMap:              db.TableMap,
			AddTime:               db.AddTime,
		}

		for chid, c := range db.ChannelMap {
			c.Lock()
			defer c.Unlock()
			data[k].ChannelMap[chid] = channelSaveInfo{
				Name:             c.Name,
				MaxThreadNum:     c.MaxThreadNum,
				CurrentThreadNum: 0,
				Status:           c.Status,
			}
		}
		logrus.Println(k, data[k])
	}
	return data
}
