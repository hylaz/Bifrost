package history

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/sirupsen/logrus"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func (history *History) threadStart(i int, wg *sync.WaitGroup) {
	defer wg.Done()
	logrus.Println("history select threadStart start:", i, history.DbName, history.SchemaName, history.TableName, " Current Select Table:", history.CurrentTableName)
	defer func() {
		logrus.Println("history select threadStart over:", i, history.DbName, history.SchemaName, history.TableName, " Current Select Table:", history.CurrentTableName)
		if err := recover(); err != nil {
			history.ThreadPool[i].Error = fmt.Errorf(fmt.Sprint(err) + string(debug.Stack()))
			logrus.Println("history select threadStart:", fmt.Sprint(err)+string(debug.Stack()))
		}
	}()
	history.Lock()
	history.ThreadPool[i] = &ThreadStatus{
		Num:       i + 1,
		Error:     nil,
		NowStartI: 0,
	}
	history.Unlock()

	db := DBConnect(history.Uri)
	defer func() {
		if err := recover(); err != nil {
			// 这里可以选择打印日志或忽略
			return
		}
		db.Close()
	}()

	db.Exec("SET NAMES utf8mb4", []driver.Value{})
	history.initMetaInfo(db)
	if len(history.Fields) == 0 {
		history.ThreadPool[i].Error = fmt.Errorf("Fields empty,%s %s %s "+history.DbName, history.SchemaName, history.TableName, " Current Select Table:", history.CurrentTableName)
		logrus.Println("history select Fields empty", history.DbName, history.SchemaName, history.TableName, " Current Select Table:", history.CurrentTableName)
		return
	}
	dbSouceInfo := server.GetDBObj(history.DbName)
	history.InitToServer()
	countChan := dbSouceInfo.GetChannel(dbSouceInfo.GetTableSelf(history.SchemaName, history.TableName).ChannelKey).GetCountChan()
	CountKey := server.GetSchemaAndTableJoin(history.SchemaName, history.TableName)
	n := len(history.Fields)
	var start uint64
	var sql string
	var rowCount int
	// 每次循环之前先累加一次，再清空统计,待协程退出的时候 ，再累加一次，这样可以避免中途退出的情况
	// 这里为什么用 闭合函数,假如放在 history 对象里，每次通过 This.TableNameArr[This.TableCountSuccess] 去获取 Table ,可能存在问题的，因为 defer 存在一定概率是在下一个表查询的时候执行呢
	StatusTable := history.TableNameArr[history.TableCountSuccess]
	var AddSelectDataCount = func() {
		StatusTable.Lock()
		StatusTable.SelectCount += uint64(rowCount)
		StatusTable.Unlock()
		history.Lock()
		history.SelectRowsCount += uint64(rowCount)
		history.Unlock()
		rowCount = 0
	}
	defer AddSelectDataCount()
	for {
		AddSelectDataCount()
		history.RLock()
		if history.Status == HISTORY_STATUS_SELECT_STOPING {
			history.RUnlock()
			break
		}
		history.RUnlock()
		sql, start = history.GetNextSql()
		if sql == "" {
			break
		}
		history.ThreadPool[i].NowStartI = start
		p := make([]driver.Value, 0)
		rows, err := db.Query(sql, p)
		if err != nil {
			logrus.Println("history select threadStart err:", err, "sql:", sql, history.DbName, history.SchemaName, history.TableName, history.CurrentTableName)
			history.ThreadPool[i].Error = err
			runtime.Goexit()
			return
		}

		for {
			history.RLock()
			if history.Status == HISTORY_STATUS_KILLED {
				history.RUnlock()
				runtime.Goexit()
				return
			}
			history.RUnlock()
			dest := make([]driver.Value, n, n)
			err := rows.Next(dest)
			if err != nil {
				break
			}
			rowCount++
			m := make(map[string]interface{}, n)
			sizeCount := int64(0)
			for i, v := range history.Fields {
				if dest[i] == nil {
					m[*v.COLUMN_NAME] = dest[i]
					continue
				}
				switch *v.DATA_TYPE {
				case "set":
					m[*v.COLUMN_NAME] = strings.Split(dest[i].(string), ",")
					break
				case "tinyint":
					if *v.COLUMN_TYPE == "tinyint(1)" {
						switch fmt.Sprint(dest[i]) {
						case "1":
							m[*v.COLUMN_NAME] = true
							break
						case "0":
							m[*v.COLUMN_NAME] = false
							break
						default:
							m[*v.COLUMN_NAME] = dest[i]
							break
						}
					} else {
						m[*v.COLUMN_NAME] = dest[i]
					}
					break
				case "json":
					var d interface{}
					json.Unmarshal([]byte(dest[i].(string)), &d)
					m[*v.COLUMN_NAME] = d
					break
				case "timestamp", "datetime", "time":
					if v.Fsp == 0 {
						m[*v.COLUMN_NAME] = dest[i]
						break
					}
					val := dest[i].(string)
					i := strings.Index(val, ".")
					if i < 0 {
						m[*v.COLUMN_NAME] = val + "." + fmt.Sprintf("%0*d", v.Fsp, 0)
						break
					}
					n := len(val[i+1:])
					if n == v.Fsp {
						m[*v.COLUMN_NAME] = val
						break
					}
					if n < v.Fsp {
						m[*v.COLUMN_NAME] = val + fmt.Sprintf("%0*d", v.Fsp-n, 0)
					} else {
						m[*v.COLUMN_NAME] = val[0 : len(val)-n+v.Fsp]
					}

				default:
					m[*v.COLUMN_NAME] = dest[i]
					break
				}
				sizeCount += int64(unsafe.Sizeof(m[*v.COLUMN_NAME]))
			}
			if len(m) == 0 {
				return
			}
			Rows := make([]map[string]interface{}, 1)
			Rows[0] = m
			d := &pluginDriver.PluginDataType{
				Timestamp:      uint32(time.Now().Unix()),
				EventType:      "insert",
				Rows:           Rows,
				Query:          "",
				SchemaName:     history.SchemaName,
				TableName:      history.CurrentTableName,
				BinlogFileNum:  0,
				BinlogPosition: 0,
				Pri:            history.TablePriArr,
				ColumnMapping:  history.ColumnMapping,
			}

			history.sendToServerResult(d)

			countChan <- &count.FlowCount{
				//Time:"",
				Count:    1,
				TableId:  CountKey,
				ByteSize: sizeCount * int64(len(history.ToServerList)),
			}
		}
		rows.Close()

		if (history.Property.LimitOptimize == 0 || history.TablePriKeyMaxId == 0) && rowCount < history.Property.ThreadCountPer {
			runtime.Goexit()
		}
	}
	runtime.Goexit()
}

func (history *History) sendToServerResult(pluginData *pluginDriver.PluginDataType) {
	for _, toServer := range history.ToServerList {
		ToServerInfo := toServer.ToServerInfo
		ToServerInfo.Lock()
		status := ToServerInfo.Status
		if status == "deling" || status == "deled" {
			ToServerInfo.Unlock()
			return
		}
		ToServerInfo.QueueMsgCount++
		if ToServerInfo.ToServerChan == nil {
			ToServerInfo.ToServerChan = &server.ToServerChan{
				To: make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
			}
		}
		// 保证只要还有数据写入，就有最小的消费进程数量还在消费
		toServer.Lock()
		if toServer.threadCount < history.Property.SyncThreadNum {
			// 为什么这里放一个协程去异步等待协程结束 ,而不是最开始初始化的时候,就启动呢
			// 假如最开始初始化就启动了一个协程,但是假如拉取数据的协程,压根就没拉到数据,那等待同步协程结束 的 协程 不就是一直阻塞在那吗?
			for i := 0; i < history.Property.SyncThreadNum-toServer.threadCount; i++ {
				//每启用一个同步协程,就 +1 每个协程结束,就相对 -1
				history.SyncWaitToServerOver(1)
				toServer.threadCount++
				go func() {
					//这里要用 defer 是因为 ConsumeToServer 里 直接用了  runtime.Goexit()
					defer func() {
						toServer.Lock()
						toServer.threadCount--
						toServer.Unlock()
						history.ToServerTheadGroup.Done()
					}()
					ToServerInfo.ConsumeToServer(server.GetDBObj(history.DbName), history.SchemaName, history.TableName)
				}()
			}
		}
		toServer.Unlock()
		ToServerInfo.Unlock()
		ToServerInfo.ToServerChan.To <- pluginData
	}
}

func (history *History) GetNextSql() (sql string, start uint64) {
	history.Lock()
	defer history.Unlock()
	var where = ""
	if history.Property.LimitOptimize == 0 || history.TablePriKeyMaxId == 0 {
		if history.Property.Where != "" {
			where = " WHERE " + history.Property.Where
		}
		start = history.NowStartI
		history.NowStartI += uint64(history.Property.ThreadCountPer)
		var limit string = ""
		// 假如没有主键 或者 非 InnoDB 引擎，直接 select *from t limit x,y
		limit = " LIMIT " + strconv.FormatUint(start, 10) + "," + strconv.Itoa(history.Property.ThreadCountPer)
		if history.TableInfo.ENGINE != "InnoDB" || history.TablePriKey == "" {
			sql = "SELECT * FROM `" + history.SchemaName + "`.`" + history.CurrentTableName + "`" + where + limit
		} else {
			// 假如有主键的情况下，采用 join 子查询的方式先分页再 通过 主键去查数据,大分页的情况下，有一定优化作用，innodb下才有效
			// 因为分页实际是找出前面的数据再丢掉，而优先对主键分页，意思只只要优先查出主键来分页就行了，丢掉的数据会大大减少
			sql = "SELECT a.* FROM `" + history.SchemaName + "`.`" + history.CurrentTableName + "` AS a "
			sql += " INNER JOIN ("
			sql += " SELECT `" + history.TablePriKey + "` FROM `" + history.SchemaName + "`.`" + history.CurrentTableName + "`" + where + limit
			sql += " ) AS b"
			sql += " on a." + history.TablePriKey + " = b." + history.TablePriKey
		}
	} else {
		// 假如TablePriKeyMaxId 有最大值，则说明 主键是 数字类型，可以通过 between 来分页
		//假如最大开始值 已经超过最大Id值了，则说明不需要再去查询了
		if history.NowStartI >= history.TablePriKeyMaxId {
			return
		}
		// BETWEEN NowStartI AND endI
		// BETWEEN 是包含边界的， 等价于  x >= NowStartI AND x <= endI
		var endI uint64
		if history.NowStartI == 0 {
			history.NowStartI = history.TablePriKeyMinId
		}
		start = history.NowStartI
		// 这里最大值 - 每次分页数量 是为了 不int内存溢出，避免 NowStartI + ThreadCountPer 大于 uint64
		// 假如 between 右区间 endI 大于 当前 This.NowStartI，则设置 This.NowStartI 为 endI+1，因为 This.NowStartI 是代表下一次查询的开始位置
		if history.TablePriKeyMaxId >= uint64(history.Property.ThreadCountPer) && history.TablePriKeyMaxId-uint64(history.Property.ThreadCountPer)-1 > history.NowStartI {
			endI = history.NowStartI + uint64(history.Property.ThreadCountPer) - 1
			history.NowStartI = endI + 1
		} else {
			endI = history.TablePriKeyMaxId
			history.NowStartI = endI
		}
		if history.Property.Where == "" {
			where = " WHERE `" + history.TablePriKey + "` BETWEEN " + strconv.FormatUint(start, 10) + " AND " + strconv.FormatUint(endI, 10)
		} else {
			where = " WHERE `" + history.TablePriKey + "` BETWEEN " + strconv.FormatUint(start, 10) + " AND " + strconv.FormatUint(endI, 10) + " AND " + history.Property.Where
		}
		sql = "SELECT * FROM `" + history.SchemaName + "`.`" + history.CurrentTableName + "` " + where
	}
	return
}
