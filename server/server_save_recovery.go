package server

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin"
	"github.com/brokercap/Bifrost/server/storage"
	"github.com/brokercap/Bifrost/server/user"
	"github.com/brokercap/Bifrost/server/warning"
	"github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

var l sync.RWMutex
var serverStartTime = time.Now()

type recovery struct {
	Version   string
	StartTime time.Time
	ToServer  *json.RawMessage
	DbInfo    *json.RawMessage
	User      *json.RawMessage
	Warning   *json.RawMessage
}

type recoveryDataSturct struct {
	Version   string
	StartTime time.Time
	ToServer  interface{}
	DbInfo    interface{}
	User      interface{}
	Warning   interface{}
}

func DoRecoverySnapshotData() {
	user.InitUser()

	fd, err := storage.GetDBInfo()
	if err != nil {
		return
	}
	if string(fd) == "" {
		return
	}

	var data recovery
	errors := json.Unmarshal(fd, &data)
	if errors != nil {
		logrus.Printf("recovery error:%s, data:%s \r\n", errors, string(fd))
		return
	}

	setServerStartTime(data.StartTime)
	if data.ToServer != nil && string(*data.ToServer) != "{}" {
		plugin.Recovery(data.ToServer)
	}

	if data.DbInfo != nil && string(*data.DbInfo) != "{}" {
		Recovery(data.DbInfo, false)
	}

	if data.User != nil && string(*data.User) != "[]" {
		user.RecoveryUser(data.User)
	}
	if data.Warning != nil && string(*data.Warning) != "{}" {
		warning.RecoveryWarning(data.Warning)
	}
}

func GetSnapshotData() ([]byte, error) {
	l.Lock()
	defer func() {
		l.Unlock()
		if err := recover(); err != nil {
			logrus.Println(err)
		}
	}()
	data := recoveryDataSturct{
		Version:   config.VERSION,
		StartTime: GetServerStartTime(),
		ToServer:  plugin.SaveToServerData(),
		DbInfo:    SaveDBInfoToFileData(),
		User:      user.GetUserList(),
		Warning:   warning.GetWarningConfigList(),
	}
	return json.Marshal(data)
}

func GetSnapshotData2() ([]byte, error) {
	l.Lock()
	defer func() {
		l.Unlock()
		if err := recover(); err != nil {
			logrus.Println(err)
		}
	}()
	data := recoveryDataSturct{
		Version:   config.VERSION,
		StartTime: GetServerStartTime(),
		ToServer:  plugin.SaveToServerData(),
		DbInfo:    SaveDBInfoToFileData(),
	}
	return json.Marshal(data)
}

// DoSaveSnapshotData 持久化保存
func DoSaveSnapshotData() {
	var data []byte
	var err error
	for i := 0; i < 3; i++ {
		data, err = GetSnapshotData2()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	if err != nil {
		SaveDBConfigInfo()
		return
	}
	storage.SaveDBInfo(data)
}

func DoRecoveryByBackupData(fileContent string) {
	var data recovery
	errors := json.Unmarshal([]byte(fileContent), &data)
	if errors != nil {
		logrus.Printf("recovery error:%s, data:%s \r\n", errors, fileContent)
		return
	}
	setServerStartTime(data.StartTime)
	if string(*data.ToServer) != "{}" {
		plugin.Recovery(data.ToServer)
	}
	if string(*data.DbInfo) != "{}" {
		Recovery(data.DbInfo, true)
	}
	if string(*data.Warning) != "{}" {
		warning.RecoveryWarning(data.Warning)
	}
	if string(*data.User) != "[]" {
		user.RecoveryUser(data.User)
	}
}

func setServerStartTime(t time.Time) {
	if t.IsZero() {
		t = GetServerStartTimeByConfigFile()
	}

	if !serverStartTime.IsZero() {
		if t.After(serverStartTime) {
			return
		}
	}
	serverStartTime = t
}

func GetServerStartTimeByConfigFile() time.Time {
	fInfo, err := os.Stat(config.BifrostConfigFile)
	if err != nil {
		return time.Now()
	}
	return fInfo.ModTime()
}
