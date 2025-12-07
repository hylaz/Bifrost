package server

import (
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/mysql"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"time"
)

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	case mysql.QUERY_EVENT:
		return "sql"
	case mysql.XID_EVENT:
		return "commit"
	default:
		break
	}
	return fmt.Sprintf("%d", e)
}

type ToServerChan struct {
	To chan *pluginDriver.PluginDataType
}

type ConsumeChannel struct {
	sync.RWMutex
	db         *db
	channel    *Channel
	schemaName string
	tableName  string
}

func newConsumeChannel(channel *Channel) *ConsumeChannel {
	return &ConsumeChannel{
		db:      channel.db,
		channel: channel,
	}
}

func (consume *ConsumeChannel) checkChannelStatus() {
	if consume.channel.Status == CLOSED {
		panic("channel closed")
	}
}

func (consume *ConsumeChannel) sendToServerResult(toServer *ToServer, pluginData *pluginDriver.PluginDataType) {
	toServer.Lock()
	status := toServer.Status
	FileQueueStatus := toServer.FileQueueStatus
	if status == DELING || status == DELED {
		toServer.Unlock()
		return
	}
	if status == DEFAULT {
		toServer.Status = RUNNING
	}
	//修改toserver 对应最后接收的 位点信息
	var lastQueueBinlog = &PositionStruct{
		BinlogFileNum:  pluginData.BinlogFileNum,
		BinlogPosition: pluginData.BinlogPosition,
		GTID:           pluginData.Gtid,
		Timestamp:      pluginData.Timestamp,
		EventID:        pluginData.EventID,
	}
	toServer.LastQueueBinlog = lastQueueBinlog

	// 支持到 1.8.x
	toServer.LastBinlogFileNum, toServer.LastBinlogPosition = pluginData.BinlogFileNum, pluginData.BinlogPosition

	//ToServerInfo.LastBinlogFileNum,ToServerInfo.LastBinlogPosition,ToServerInfo.LastBinlogGtid,ToServerInfo.LastBinlogEventID = pluginData.BinlogFileNum,pluginData.BinlogPosition,pluginData.Gtid,pluginData.EventID
	if toServer.ToServerChan == nil {
		toServer.ToServerChan = &ToServerChan{
			To: make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
		}
		go toServer.consume_to_server(consume.db, pluginData.SchemaName, pluginData.TableName)
	}
	toServer.Unlock()
	if toServer.LastBinlogKey == nil {
		toServer.LastBinlogKey = getToServerLastBinlogkey(consume.db, toServer)
	}
	saveBinlogPositionByCache(toServer.LastBinlogKey, lastQueueBinlog)
	if FileQueueStatus {
		toServer.InitFileQueue(consume.db.Name, pluginData.SchemaName, pluginData.TableName)
		toServer.AppendToFileQueue(pluginData)
		return
	}

	// 假如开启了全局文件队列的功能,假如 规定时间内 都没写进内存chan队列,则往文件队列中写数据
	if config.FileQueueUsable {
		timer := time.NewTimer(time.Duration(config.FileQueueUsableCountTimeDiff) * time.Millisecond)
		defer timer.Stop()
		select {
		case toServer.ToServerChan.To <- pluginData:
			toServer.Lock()
			toServer.QueueMsgCount++
			if int(toServer.QueueMsgCount) >= config.ToServerQueueSize {
				toServer.FileQueueUsableCount++
				if toServer.FileQueueUsableCount == 1 {
					toServer.FileQueueUsableCountStartTime = time.Now().UnixNano() / 1e6
				} else {
					// 假如在 FileQueueUsableCountTimeDiff 时间 内 内存队列 被挤满的次数大于 配置的 FileQueueUsableCount 大小，则认为 需要启动文件队列
					// 否则重新开始计算
					if time.Now().UnixNano()/1e6-toServer.FileQueueUsableCountStartTime > config.FileQueueUsableCountTimeDiff {
						if toServer.FileQueueUsableCount > config.FileQueueUsableCount {
							toServer.FileQueueStatus = true
						} else {
							toServer.FileQueueUsableCount = 0
						}
					}
				}
			}
			toServer.Unlock()
			break
		case <-timer.C:
			toServer.Lock()
			defer toServer.Unlock()
			toServer.InitFileQueue(consume.db.Name, consume.schemaName, consume.tableName)
			toServer.AppendToFileQueue(pluginData)
			toServer.FileQueueStatus = true
			//log.Println("start FileQueueStatus = true;",*pluginData)
			break
		}
	} else {
		toServer.ToServerChan.To <- pluginData
		toServer.Lock()
		toServer.QueueMsgCount++
		toServer.Unlock()
	}

}

func (consume *ConsumeChannel) transferToPluginData(data *mysql.EventReslut) (pluginData *pluginDriver.PluginDataType) {
	i := strings.IndexAny(data.BinlogFileName, ".")
	intString := data.BinlogFileName[i+1:]
	BinlogFileNum, _ := strconv.Atoi(intString)
	pluginData = &pluginDriver.PluginDataType{
		Timestamp:      data.Header.Timestamp,
		EventType:      evenTypeName(data.Header.EventType),
		SchemaName:     data.SchemaName,
		TableName:      data.TableName,
		Rows:           data.Rows,
		BinlogFileNum:  BinlogFileNum,
		BinlogPosition: data.Header.LogPos,
		Query:          data.Query,
		Gtid:           data.Gtid,
		Pri:            data.Pri,
		ColumnMapping:  data.ColumnMapping,
		EventID:        data.EventID,
	}
	return
}

func (consume *ConsumeChannel) consumeChannel() {
	channel := consume.channel
	var pluginData *pluginDriver.PluginDataType
	logrus.Println("channel", channel.Name, " consume_channel start")
	timer := time.NewTimer(5 * time.Second)
	defer func() {
		logrus.Println("channel", channel.Name, " consume_channel over; CurrentThreadNum:", channel.CurrentThreadNum)
		timer.Stop()
	}()

	var key string
	var allTableKey string
	var countNum int64 = 0
	var eventSize int64 = 0
	for {
		select {

		case pluginData = <-consume.channel.chanName:
			if consume.db.killStatus == 1 {
				return
			}
			consume.checkChannelStatus()

			switch pluginData.EventType {
			case "update":
				countNum = int64(len(pluginData.Rows) / 2)
				break
			case "sql", "commit":
				countNum = 0
				break
			default:
				countNum = int64(len(pluginData.Rows))
				break
			}

			eventSize = int64(pluginData.EventSize)

			key = GetSchemaAndTableJoin(pluginData.AliasSchemaName, pluginData.AliasTableName)
			allTableKey = GetSchemaAndTableJoin(pluginData.AliasSchemaName, "*")

			consume.schemaName, consume.tableName = pluginData.AliasSchemaName, pluginData.AliasTableName
			consume.sendToServerList(key, pluginData, countNum, eventSize)

			consume.schemaName, consume.tableName = pluginData.AliasSchemaName, "*"
			consume.sendToServerList(allTableKey, pluginData, countNum, eventSize)

			consume.schemaName, consume.tableName = "*", "*"
			consume.sendToServerList(AllSchemaAndTablekey, pluginData, countNum, eventSize)

			if consume.db.killStatus == 1 {
				return
			}

			timer.Reset(5 * time.Second)
		case <-timer.C:
			timer.Reset(5 * time.Second)
		}

		for {
			if channel.Status == STOPPED {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}

		if channel.CurrentThreadNum > channel.MaxThreadNum || channel.Status == CLOSED {
			channel.CurrentThreadNum--
			break
		}

	}
}

func (consume *ConsumeChannel) checkIgnoreTable(table *Table, TableName string) bool {
	consume.db.RLock()
	defer consume.db.RUnlock()

	if len(table.doTableMap) > 0 {
		if _, ok := table.doTableMap[TableName]; ok {
			return false
		}
		return true
	}
	if _, ok := table.ignoreTableMap[TableName]; ok {
		return true
	}
	return false
}

func (consume *ConsumeChannel) sendToServerList(key string, pluginData *pluginDriver.PluginDataType, countNum, eventSize int64) {
	table := consume.db.GetTableByKey(key)
	if table == nil {
		return
	}

	if consume.checkIgnoreTable(table, pluginData.TableName) == false {
		if len(table.ToServerList) > 0 {
			consume.toServerList(table.ToServerList, pluginData)
			consume.channel.countChan <- &count.FlowCount{
				Count:    countNum,
				TableId:  table.key,
				ByteSize: eventSize * int64(len(table.ToServerList)),
			}
		}
	}

	for _, joinTable := range table.likeTableList {
		if consume.checkIgnoreTable(joinTable, pluginData.TableName) == true {
			continue
		}
		consume.toServerList(joinTable.ToServerList, pluginData)
		consume.channel.countChan <- &count.FlowCount{
			Count:    countNum,
			TableId:  joinTable.key,
			ByteSize: eventSize * int64(len(joinTable.ToServerList)),
		}
	}
}

func (consume *ConsumeChannel) toServerList(toServerList []*ToServer, pluginData *pluginDriver.PluginDataType) {
	for _, toServerInfo := range toServerList {

		if toServerInfo.FilterQuery && pluginData.EventType == "sql" {
			if pluginData.Query != "COMMIT" {
				continue
			}
		}

		if pluginData.EventID < toServerInfo.LastSuccessBinlog.EventID {
			if pluginData.Timestamp < toServerInfo.LastSuccessBinlog.Timestamp {
				continue
			}
		}

		consume.sendToServerResult(toServerInfo, pluginData)
	}
}
