package server

import (
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/warning"
	"github.com/sirupsen/logrus"
	"io"
	"runtime"
	"runtime/debug"
	"time"
)

// Stop 暂停操作
func (server *ToServer) Stop() {
	server.Lock()
	defer server.Unlock()
	if server.Status == "" {
		logrus.Println("ToServer ", server.Key, server.ToServerKey, server.ToServerID, " stopped")
		server.Status = STOPPED
		return
	}
	if server.Status == RUNNING {
		logrus.Println("ToServer ", server.Key, server.ToServerKey, server.ToServerID, " stopping")
		server.Status = STOPPING
		return
	}
}

// Start 暂停后重新启动操作
func (server *ToServer) Start() {
	server.Lock()
	defer server.Unlock()
	if server.Status == STOPPED {
		if server.ThreadCount == 0 {
			server.Status = DEFAULT
		} else {
			server.StatusChan <- true
		}
		logrus.Println("ToServer ", server.Key, server.ToServerKey, server.ToServerID, " start")
	}
}

func (server *ToServer) ConsumeToServer(db *db, schemaName string, tableName string) {
	server.consumeToServer(db, schemaName, tableName)
}

func (server *ToServer) consumeToServer(db *db, SchemaName string, TableName string) {
	var MyConsumerId int
	server.Lock()
	if server.ConsumePluginParamArr == nil {
		server.ConsumePluginParamArr = make([]interface{}, 0)
	}
	server.ThreadCount++
	server.ConsumePluginParamArr = append(server.ConsumePluginParamArr, nil)
	MyConsumerId = len(server.ConsumePluginParamArr) - 1
	//强制给参数 加入  BifrostMustBeSuccess 保留参数字段
	if server.PluginParam != nil {
		server.PluginParam["BifrostMustBeSuccess"] = server.MustBeSuccess
		server.PluginParam["BifrostFilterQuery"] = server.FilterQuery
	}
	server.Unlock()
	toServerPositionBinlogKey := getToServerBinlogKey(db, server)
	// 因为有多个地方对 ThreadCount - 1 操作，记录是否已经扣减过
	var ThreadCountDecrDone bool = false
	defer func() {
		if err := recover(); err != nil {
			logrus.Println(db.Name, server.Notes, "toServerKey:", server.Key, "MyConsumerId:", MyConsumerId, "SchemaName:", SchemaName, "TableName:", TableName, server.PluginName, server.ToServerKey, "ToServer consume_to_server over;err:", err, "debug", string(debug.Stack()))
			return
		} else {
			logrus.Println(db.Name, server.Notes, "toServerKey:", server.Key, "MyConsumerId:", MyConsumerId, "SchemaName:", SchemaName, "TableName:", TableName, server.PluginName, server.ToServerKey, "ToServer consume_to_server over")
		}

		server.Lock()
		if ThreadCountDecrDone == false {
			server.ThreadCount--
		}
		if server.ThreadCount == 0 {
			server.ConsumePluginParamArr = nil
		} else {
			server.ConsumePluginParamArr[MyConsumerId] = nil
		}
		server.Unlock()
	}()

	logrus.Println(db.Name, server.Notes, "toServerKey:", server.Key, "MyConsumerId:", MyConsumerId, "SchemaName:", SchemaName, "TableName:", TableName, server.PluginName, server.ToServerKey, "ToServer consume_to_server  start")
	c := server.ToServerChan.To
	server.Lock()
	if server.Status == DEFAULT {
		server.Status = RUNNING
	}
	server.Unlock()
	var CheckStatusFun = func() {
		for {
			if db.killStatus == 1 {
				runtime.Goexit()
			}
			server.Lock()
			switch server.Status {
			case DELING:
				server.Status = DELED
				delBinlogPosition(toServerPositionBinlogKey)
				server.Unlock()
				runtime.Goexit()
				break
			case STOPPING, STOPPED:
				if server.Status == STOPPING {
					//检测是否需要在暂停操作，如果是暂停操作，则修改为已暂停状态，并且等待开启
					server.Status = STOPPED
					logrus.Println("ToServer ", server.Key, server.ToServerKey, server.ToServerID, " stopped")
				}
				server.Unlock()
				select {
				case <-server.StatusChan:
					server.Lock()
					server.Status = RUNNING
					server.Unlock()
					return
				case <-time.NewTimer(5 * time.Second).C:
					break
				}
			default:
				server.Unlock()
				return
			}
		}
	}
	var LastSuccessData *pluginDriver.PluginDataType
	var ErrData *pluginDriver.PluginDataType
	var errs error
	binlogKey := getToServerBinlogKey(db, server)

	var SaveBinlog = func() {
		if LastSuccessData != nil {
			switch LastSuccessData.EventType {
			case "commit", "sql":
				break
			default:
				return
			}
			var LastSuccessBinlog = &PositionStruct{
				BinlogFileNum:  LastSuccessData.BinlogFileNum,
				BinlogPosition: LastSuccessData.BinlogPosition,
				GTID:           LastSuccessData.Gtid,
				Timestamp:      LastSuccessData.Timestamp,
				EventID:        LastSuccessData.EventID,
			}

			server.LastSuccessBinlog = LastSuccessBinlog
			saveBinlogPositionByCache(binlogKey, LastSuccessBinlog)

			// 支持到 1.8.x
			server.BinlogFileNum = LastSuccessData.BinlogFileNum
			server.BinlogPosition = LastSuccessData.BinlogPosition
		}
	}
	var fordo int8 = 0
	var lastErrTime int64 = 0
	var warningStatus bool = false

	//告警方法
	var doWarningFun = func(warningType warning.WarningType, body string) {
		if warningType == warning.WARNINGNORMAL && warningStatus != true {
			return
		}
		warningStatus = true
		warning.AppendWarning(warning.WarningContent{
			Type:       warningType,
			DbName:     db.Name,
			SchemaName: SchemaName,
			TableName:  TableName,
			Body:       body,
		})
	}
	var noData bool = true

	var unack int = 0                                    // 在一次遍历中从文件队列中加载出来的数量，需要
	var tmpUnack int = 0                                 // 在一次遍历中从文件队列中加载出来 但是实际位点是被成功处理过后的 数量
	var lastFromFileEndData *pluginDriver.PluginDataType // 从文件队列中加载出来的最后 最后一条数据
	var fileTotalCount int = 0
	var fileAck = func() {
		if LastSuccessData == nil {
			return
		}
		if unack == 0 {
			return
		}
		if server.FileQueueObj == nil {
			unack = 0
			return
		}
		if LastSuccessData.BinlogFileNum == 0 || LastSuccessData.BinlogPosition == 0 {
			return
		}
		//假如 最后成功的位点，大于文件中加载的位点，则将所有待从文件中的数量  ack 掉
		if LastSuccessData.BinlogFileNum > lastFromFileEndData.BinlogFileNum {
			//logrus.Println("file ackn:",unack," fileTotalCount:",fileTotalCount)
			server.FileQueueObj.Ack(unack)
			unack = 0
			return
		}
		if LastSuccessData.BinlogFileNum == lastFromFileEndData.BinlogFileNum {
			if LastSuccessData.BinlogPosition >= lastFromFileEndData.BinlogPosition {
				server.FileQueueObj.Ack(unack)
				unack = 0
			} else {
				server.FileQueueObj.Ack(1)
				unack--
			}
		} else {
			return
		}
	}
	var checkDoWarning = func() {
		// lastErrTime 是指第一次错误的时间,假如报警过后,将 lastErrTime 修改为1小时后,这样就可以实现最近一小时,同一个错误不会重复报警了
		if time.Now().Unix()-lastErrTime >= 30 {
			lastErrTime = time.Now().Unix() + 3600
			doWarningFun(warning.WARNINGERROR, "PluginName:"+server.PluginName+";ToServerKey:"+server.ToServerKey+" err:"+errs.Error())
		}
	}
	var retry = false
	var checkDealSkipErrData = func() bool {
		// 假如不是第一次循环,尝试 获取 错误信息,是否要被过滤掉,如果要被过滤掉,则退出循环
		dealStatus := server.GetWaitErrorDeal()
		if dealStatus == 1 {
			// 假如手工点击了 位点错过,则通过插件层，要执行跳过位点
			if server.SkipBinlog(MyConsumerId, ErrData) != nil {
				return false
			}
			server.DelWaitError()
			lastErrTime = 0
			//人工处理恢复
			doWarningFun(warning.WARNINGNORMAL, "Return to normal by user")
			return true
		}
		return false
	}
	var forSendData = func(data *pluginDriver.PluginDataType) {
		retry = false
		for {
			errs = nil
			LastSuccessData, ErrData, errs = server.sendToServer(data, MyConsumerId, retry)
			if server.MustBeSuccess == true {
				if errs == nil {
					if lastErrTime > 0 {
						server.DelWaitError()
						lastErrTime = 0
						//自动恢复
						doWarningFun(warning.WARNINGNORMAL, "Automatically return to normal")
					}
					fileAck()
					break
				}

				// err != nil 逻辑
				// 假如 lastErrTime == 0 代表已经是第一次循环尝试,则需要记录 错误时间
				server.AddWaitError(errs, ErrData)
				if lastErrTime == 0 {
					fordo = 0
					lastErrTime = time.Now().Unix()
				} else {
					if checkDealSkipErrData() {
						break
					}
				}
				fordo++
				// 每重试2次,进行阻塞休眠一次
				if fordo == 2 {
					fordo = 0
					CheckStatusFun()
					timer2 := time.NewTimer(time.Duration(config.PluginSyncRetrycTime) * time.Second)
					<-timer2.C
					timer2.Stop()
					checkDoWarning()
				}
				retry = true
			} else {
				LastSuccessData = nil
				fileAck()
				break
			}
		}
	}

	var n1 int = 0
	var n0 int = 0
	var timer *time.Timer
	timer = time.NewTimer(time.Duration(config.PluginCommitTimeOut) * time.Second)
	defer timer.Stop()
	for {
		CheckStatusFun()
		if server.FileQueueStatus && server.QueueMsgCount == 0 {
			//这要问我这里为什么 -1, 因为我不知道 在同一个线程里写满后再消费，会不会进入 chan 死锁的情况
			queueVariableSize := config.ToServerQueueSize - 1
			if queueVariableSize > 0 {
				server.InitFileQueue(db.Name, SchemaName, TableName)
				//logrus.Println("file ack2:",unack," fileTotalCount:",fileTotalCount)
				server.FileQueueObj.Ack(unack)
				tmpUnack = 0
				unack = 0
				logrus.Println(db.Name, SchemaName, TableName, server.PluginName, server.ToServerKey, "ToServer consume_to_server start PopFileQueue")
				var err error
				for i := 0; i < queueVariableSize; i++ {
					var data0 *pluginDriver.PluginDataType
					data0, err = server.PopFileQueue()
					if err != nil && err != io.EOF {
						doWarningFun(warning.WARNINGERROR, "PluginName:"+server.PluginName+";ToServerKey:"+server.ToServerKey+";dbName:"+db.Name+";SchemaName:"+SchemaName+";TableName:"+TableName+"; PopFileQueue err:"+err.Error())
						logrus.Println(db.Name, SchemaName, TableName, ";ToServerKey:"+server.ToServerKey, " PopFileQueue err:", err, " restart Bifrost please!")
						panic("PluginName:" + server.PluginName + ";ToServerKey:" + server.ToServerKey + ";dbName:" + db.Name + ";SchemaName:" + SchemaName + ";TableName:" + TableName + "; PopFileQueue err:" + err.Error())
					}
					if data0 == nil && err == nil {
						// 说明没有数据可以加载了
						server.Lock()
						server.FileQueueStatus = false
						server.Unlock()
						break
					} else {
						/*
							if i == 0{
								logrus.Println("PopFileQueue first: ",*data0)
							}
						*/
						// 这里为什么要判断一下位点，是因为文件队列是要整个文件的数据都被从加载到内存后才会 删除文件
						// 那有一种可能，一个文件还没被完全加载完，进程就被重启了呢？那重启后，是不是旧的数据会被重新读取吗？
						if data0.BinlogFileNum < server.BinlogFileNum {
							tmpUnack++
							continue
						}
						if data0.BinlogFileNum == server.BinlogFileNum && data0.BinlogPosition <= server.BinlogPosition {
							tmpUnack++
							continue
						}
						lastFromFileEndData = data0
						unack++
						fileTotalCount++
						server.QueueMsgCount++
						c <- data0
					}
				}
				server.FileQueueObj.Ack(tmpUnack)
				//假如这一次循环加载出来的数据，全是已经同步过的，则继续从文件中加载
				if unack == 0 {
					continue
				}
			}
		}
		var data *pluginDriver.PluginDataType
		timer.Reset(time.Duration(config.PluginCommitTimeOut) * time.Second)
		select {
		case data = <-c:
			server.Lock()
			server.QueueMsgCount--
			server.Unlock()
			noData = false
			CheckStatusFun()
			warningStatus = false
			timer.Stop()
			switch data.EventType {
			case "sql":
				forSendData(data)
				break
			case "insert", "delete":
				n1 = len(data.Rows)
				if n1 > 1 {
					n0 = 0
					for _, v := range data.Rows {
						n0++
						d := &pluginDriver.PluginDataType{
							Timestamp:      data.Timestamp,
							EventType:      data.EventType,
							Query:          "",
							SchemaName:     data.SchemaName,
							TableName:      data.TableName,
							BinlogFileNum:  0,
							BinlogPosition: 0,
							Rows:           make([]map[string]interface{}, 1),
							Gtid:           data.Gtid,
							Pri:            data.Pri,
							ColumnMapping:  data.ColumnMapping,
							EventID:        data.EventID,
						}
						if n0 == n1 {
							d.BinlogFileNum = data.BinlogFileNum
							d.BinlogPosition = data.BinlogPosition
						}
						d.Rows[0] = v
						forSendData(d)
					}
				} else {
					forSendData(data)
				}
				break
			case "update":
				n1 = len(data.Rows)
				if n1 > 2 {
					for n0 = 0; n0 < n1; n0 += 2 {
						d := &pluginDriver.PluginDataType{
							Timestamp:      data.Timestamp,
							EventType:      data.EventType,
							Query:          "",
							SchemaName:     data.SchemaName,
							TableName:      data.TableName,
							BinlogFileNum:  0,
							BinlogPosition: 0,
							Rows:           make([]map[string]interface{}, 2),
							Gtid:           data.Gtid,
							Pri:            data.Pri,
							ColumnMapping:  data.ColumnMapping,
							EventID:        data.EventID,
						}
						if n0 == n1-2 {
							d.BinlogFileNum = data.BinlogFileNum
							d.BinlogPosition = data.BinlogPosition
						}
						d.Rows[0] = data.Rows[n0]
						d.Rows[1] = data.Rows[n0+1]
						forSendData(d)
					}
				} else {
					forSendData(data)
				}
				break
			default:
				forSendData(data)
				break
			}
			//这里保存位点，为是了显示的时候，可以直接从内存中读取
			SaveBinlog()
			break
		case <-timer.C:
			timer.Stop()
			LastSuccessData, ErrData, errs = server.timeOutCommit(MyConsumerId)
			if errs == nil {
				if lastErrTime > 0 {
					server.DelWaitError()
					lastErrTime = 0
					//自动恢复
					doWarningFun(warning.WARNINGNORMAL, "Commit Automatically return to normal")
				}
			} else {
				server.AddWaitError(errs, ErrData)
				if server.MustBeSuccess && lastErrTime == 0 {
					lastErrTime = time.Now().Unix()
					checkDoWarning()
				}
				if lastErrTime > 0 {
					checkDealSkipErrData()
				}
			}
			if noData == false {
				noData = true
				logrus.Println("consume_to_server:", server.Notes, "toServerKey:", server.Key, "MyConsumerId:", MyConsumerId, server.PluginName, server.ToServerKey, server.ToServerID, " start no data")
			}
			fileAck()
			if LastSuccessData == nil && errs == nil {
				server.Lock()
				if server.QueueMsgCount == 0 {
					// 在全量任务的时候，有可能是起多个消费者,所以这里要判断一下，是不是只剩下一个消费者，只有一个消费者的时候的时候,再将 chan 关闭
					if server.ThreadCount == 1 {
						server.ToServerChan = nil
						server.Status = ""
					}
					//这里要执行一次fileAck ，是为了最终数据一致，将已经从文件中加载出来的数据 ack掉
					LastSuccessData = lastFromFileEndData
					fileAck()
					// 这里先减一次 This.ThreadCount - 1,是为了防止，defer 执行延时, 其他协程  在进入这个逻辑的时候，继续获取到的值是还没被 -1 的
					server.ThreadCount--
					ThreadCountDecrDone = true
					server.Unlock()
					runtime.Goexit()
				}
				server.Unlock()
			}
			SaveBinlog()
			break
		}
	}
}

func (server *ToServer) filterField(data *pluginDriver.PluginDataType) (newData *pluginDriver.PluginDataType, b bool) {

	if len(server.FieldList) == 0 {
		return data, true
	}

	n := len(data.Rows)
	if n == 0 {
		return data, true
	}

	if n == 1 {

		m := make(map[string]interface{})
		for _, key := range server.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				m[key] = data.Rows[0][key]
			}
		}

		newData = &pluginDriver.PluginDataType{
			Timestamp:      data.Timestamp,
			EventType:      data.EventType,
			SchemaName:     data.SchemaName,
			TableName:      data.TableName,
			BinlogFileNum:  data.BinlogFileNum,
			BinlogPosition: data.BinlogPosition,
			Rows:           make([]map[string]interface{}, 1),
			Gtid:           data.Gtid,
			Pri:            data.Pri,
			ColumnMapping:  data.ColumnMapping,
			EventID:        data.EventID,
		}
		newData.Rows[0] = m

	} else {

		newData = &pluginDriver.PluginDataType{
			Timestamp:      data.Timestamp,
			EventType:      data.EventType,
			SchemaName:     data.SchemaName,
			TableName:      data.TableName,
			BinlogFileNum:  data.BinlogFileNum,
			BinlogPosition: data.BinlogPosition,
			Rows:           make([]map[string]interface{}, 2),
			Gtid:           data.Gtid,
			Pri:            data.Pri,
			ColumnMapping:  data.ColumnMapping,
			EventID:        data.EventID,
		}
		mBefore := make(map[string]interface{})
		mAfter := make(map[string]interface{})
		var isNotUpdate = true
		for _, key := range server.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				mBefore[key] = data.Rows[0][key]
				mAfter[key] = data.Rows[1][key]
				if server.FilterUpdate {
					switch mAfter[key].(type) {
					case []string:
						m1 := mBefore[key].([]string)
						m2 := mAfter[key].([]string)
						n1 := len(m1)
						n2 := len(m2)
						if n1 != n2 {
							isNotUpdate = false
							break
						}
						for k, v := range m1 {
							if m2[k] != v {
								isNotUpdate = false
								break
							}
						}
						break
					case map[string]interface{}, map[interface{}]interface{}, map[int]interface{}, []int, []interface{}, []map[string]interface{}, []map[interface{}]interface{}:
						isNotUpdate = false
					default:
						if mBefore[key] != mAfter[key] {
							isNotUpdate = false
						}
						break
					}
				}
			}
		}

		//假如所有字段内容都未变更，并且过滤了这个功能，则直接返回false
		if isNotUpdate && server.FilterUpdate {
			return data, false
		}
		newData.Rows[0] = mBefore
		newData.Rows[1] = mAfter
	}
	return newData, true
}

// 从插件实例池中获取一个插件实例
func (server *ToServer) getPluginAndSetParam(myConsumerId int) (pluginConn *plugin.ToServerConn, err error) {
	pluginConn = plugin.GetPlugin(server.ToServerKey)
	if pluginConn == nil {
		return nil, fmt.Errorf("Get Plugin:" + server.PluginName + " ToServerKey:" + server.ToServerKey + " err,return nil")
	}

	server.Lock()
	defer server.Unlock()

	if server.ConsumePluginParamArr[myConsumerId] == nil {
		server.ConsumePluginParamArr[myConsumerId], err = pluginConn.GetConn().SetParam(server.PluginParam)
	} else {
		_, err = pluginConn.GetConn().SetParam(server.ConsumePluginParamArr[myConsumerId])
	}
	return
}

// timeOutCommit 超时
func (server *ToServer) timeOutCommit(myConsumerId int) (lastSuccessCommitData *pluginDriver.PluginDataType, errData *pluginDriver.PluginDataType, err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("ToServer:%s Commit Debug Err:%s", server.ToServerKey, string(debug.Stack()))
			logrus.Println(server.ToServerKey, "sendToServer err:", err)
		}
	}()

	pluginConn, err := server.getPluginAndSetParam(myConsumerId)
	if err != nil {
		return nil, nil, err
	}
	if pluginConn != nil {
		defer plugin.BackPlugin(pluginConn)
	}

	lastSuccessCommitData, errData, err = pluginConn.GetConn().TimeOutCommit()
	return
}

// SkipBinlog 跳过位点
func (server *ToServer) SkipBinlog(myConsumerId int, skipErrData *pluginDriver.PluginDataType) (err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("ToServer:%s Commit Debug Err:%s", server.ToServerKey, string(debug.Stack()))
			logrus.Println(server.ToServerKey, "sendToServer err:", err)
		}
	}()

	pluginConn, err := server.getPluginAndSetParam(myConsumerId)
	if err != nil {
		return err
	}
	defer plugin.BackPlugin(pluginConn)
	err = pluginConn.GetConn().Skip(skipErrData)
	return
}

func (server *ToServer) sendToServer(paramData *pluginDriver.PluginDataType, myConsumerId int, retry bool) (lastSuccessCommitData *pluginDriver.PluginDataType, errData *pluginDriver.PluginDataType, err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("sendToServer:%s Commit Debug Err:%s", server.ToServerKey, string(debug.Stack()))
			logrus.Println(server.ToServerKey, err2, err)
		}
	}()

	// 只有所有字段内容都没有更新，并且开启了过滤功能的情况下，才会返回false
	data, b := server.filterField(paramData)
	if !b {
		return paramData, nil, nil
	}

	pluginConn, err := server.getPluginAndSetParam(myConsumerId)
	if err != nil {
		return lastSuccessCommitData, data, err
	}
	defer plugin.BackPlugin(pluginConn)

	switch data.EventType {
	case "insert":
		lastSuccessCommitData, errData, err = pluginConn.GetConn().Insert(data, retry)
		break
	case "update":
		lastSuccessCommitData, errData, err = pluginConn.GetConn().Update(data, retry)
		break
	case "delete":
		lastSuccessCommitData, errData, err = pluginConn.GetConn().Del(data, retry)
		break
	case "sql":
		if data.Query == "COMMIT" {
			lastSuccessCommitData, errData, err = pluginConn.GetConn().Commit(data, retry)
		} else {
			lastSuccessCommitData, errData, err = pluginConn.GetConn().Query(data, retry)
		}
		break
	case "commit":
		lastSuccessCommitData, errData, err = pluginConn.GetConn().Commit(data, retry)
		break
	default:
		break
	}
	return
}
