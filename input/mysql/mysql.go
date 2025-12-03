package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	mysql2 "github.com/brokercap/Bifrost/mysql"
	"log"
	"sync"
)

var MySQLBinlogDump string

type MysqlInput struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo        inputDriver.InputInfo
	binlogDump       *mysql2.BinlogDump
	reslut           chan error
	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64
	callback         inputDriver.Callback

	replicateDoDb map[string]map[string]bool
}

func NewInputPlugin() inputDriver.Driver {
	return &MysqlInput{}
}

func (c *MysqlInput) GetUriExample() (string, string) {
	notesHtml := `
		<p><span class="help-block m-b-none">授权权限例子：GRANT SELECT, SHOW DATABASES, SUPER, REPLICATION SLAVE, EVENT ON *.* TO 'xxtest'@'%'</span></p>
		<p><span class="help-block m-b-none">RDS云产品数据库云产品权限,可能不是按 MySQL 开源权限来的,请自行确认是否有足够权限(不要勾选 验证是否有足够权限 选项)</span></p>
		<p><span class="help-block m-b-none">自确认权限：kill 当前帐号的连接, SET 命令权限,SHOW EVENT 权限 等</span></p>
		<p><span class="help-block m-b-none">需要SQL权限细节,请参考 <a href="/docs" target="_blank">DOC文档</a></span></p>
	`
	return "root:root@tcp(127.0.0.1:3306)/test", notesHtml
}

func (c *MysqlInput) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	c.inputInfo = inputInfo
}

func (c *MysqlInput) Start(ch chan *inputDriver.PluginStatus) error {
	switch c.status {
	case inputDriver.STOPPING, inputDriver.STOPPED:
		return c.Start1()
	default:
		c.PluginStatusChan = ch
		return c.Start0()
	}
	return nil
}

func (c *MysqlInput) Start0() error {
	c.reslut = make(chan error, 1)
	c.binlogDump = mysql2.NewBinlogDump(
		c.inputInfo.ConnectUri,
		c.MySQLCallback,
		[]mysql2.EventType{
			mysql2.WRITE_ROWS_EVENTv2, mysql2.UPDATE_ROWS_EVENTv2, mysql2.DELETE_ROWS_EVENTv2,
			mysql2.QUERY_EVENT,
			mysql2.XID_EVENT,
			mysql2.WRITE_ROWS_EVENTv1, mysql2.UPDATE_ROWS_EVENTv1, mysql2.DELETE_ROWS_EVENTv1,
			mysql2.WRITE_ROWS_EVENTv0, mysql2.UPDATE_ROWS_EVENTv0, mysql2.DELETE_ROWS_EVENTv0,
		},
		nil, nil)
	c.binlogDump.SetNextEventID(c.eventID)
	c.InitBinlogDumpReplicateDoDb()
	if !c.inputInfo.IsGTID || c.inputInfo.GTID == "" {
		go c.binlogDump.StartDumpBinlog(c.inputInfo.BinlogFileName, c.inputInfo.BinlogPostion, c.inputInfo.ServerId, c.reslut, c.inputInfo.MaxFileName, c.inputInfo.MaxPosition)
	} else {
		log.Println("c.inputInfo.GTID:", c.inputInfo.GTID, " c.inputInfo.ServerId:", c.inputInfo.ServerId)
		go c.binlogDump.StartDumpBinlogGtid(c.inputInfo.GTID, c.inputInfo.ServerId, c.reslut)
	}
	go c.monitorDump()
	return nil
}

func (c *MysqlInput) Start1() error {
	c.binlogDump.Start()
	return nil
}

func (c *MysqlInput) monitorDump() (r bool) {
	defer func() {
		if err := recover(); err != nil {
			// 上一层 PluginStatusChan 在进程退出之前会被关闭，这里需要无视异常情况
		}
	}()
	for {
		select {
		case v := <-c.reslut:
			if v == nil {
				return
			}
			switch v.Error() {
			case "stop":
				c.status = inputDriver.STOPPED
				break
			case "running":
				c.status = inputDriver.RUNNING
				c.err = nil
				break
			case "starting":
				c.status = inputDriver.STARTING
				break
			case "close":
				c.status = inputDriver.CLOSED
				c.err = nil
				c.PluginStatusChan <- &inputDriver.PluginStatus{Status: c.status, Error: c.err}
				return
			default:
				c.status = inputDriver.CLOSED
				c.err = v
				break
			}
			break
		}
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status: c.status, Error: c.err}
	}
	return true
}

func (c *MysqlInput) Stop() error {
	c.binlogDump.Stop()
	return nil
}

func (c *MysqlInput) Close() error {
	c.binlogDump.Close()
	return nil
}

func (c *MysqlInput) Kill() error {
	c.binlogDump.KillDump()
	return nil
}

func (c *MysqlInput) GetLastPosition() *inputDriver.PluginPosition {
	FileName, Position, Timestamp, GTID, LastEventID := c.binlogDump.GetBinlog()
	if FileName == "" {
		return nil
	}
	return &inputDriver.PluginPosition{
		GTID:           GTID,
		BinlogFileName: FileName,
		BinlogPostion:  Position,
		Timestamp:      Timestamp,
		EventID:        LastEventID,
	}
}

func (c *MysqlInput) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *MysqlInput) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}
