package server

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/filequeue"
	"github.com/sirupsen/logrus"
)

func GetFileQueue(dbName, schemaName, tableName, toServerID string) string {
	return config.DataDir + "/filequeue/" + dbName + "/" + schemaName + "/" + tableName + "/" + toServerID
}

// InitFileQueue 初始化文件队列
func (server *ToServer) InitFileQueue(dbName, schemaName, tableName string) *ToServer {
	if server.FileQueueObj == nil {
		server.FileQueueObj = filequeue.NewQueue(GetFileQueue(dbName, schemaName, tableName, fmt.Sprint(server.ToServerID)))
	}
	return server
}

// AppendToFileQueue 将数据刷到磁盘队列中
func (server *ToServer) AppendToFileQueue(data *pluginDriver.PluginDataType) error {
	v, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return server.FileQueueObj.AppendBytes(v)
}

// PopFileQueue 从磁盘队列中取出最前面一条数据
func (server *ToServer) PopFileQueue() (*pluginDriver.PluginDataType, error) {
	v, err := server.FileQueueObj.Pop()
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	var data pluginDriver.PluginDataType
	err = json.Unmarshal(v, &data)
	if err != nil {
		logrus.Println("fileQueueObj err data:", string(v))
		return nil, err
	}
	return &data, nil
}

// ReadLastFromFileQueue 从磁盘队列中取出最后面一条数据
func (server *ToServer) ReadLastFromFileQueue() (*pluginDriver.PluginDataType, error) {
	v, err := server.FileQueueObj.ReadLast()
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	var data pluginDriver.PluginDataType
	err = json.Unmarshal(v, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

// FileQueueStart 文件队列启用
func (server *ToServer) FileQueueStart() error {
	server.Lock()
	defer server.Unlock()
	if config.FileQueueUsable == true {
		server.FileQueueStatus = true
	} else {
		return fmt.Errorf("config.FileQueueUsable unable")
	}
	return nil
}

// GetFileQueueInfo 查看文件队列基本信息
func (server *ToServer) GetFileQueueInfo() (info filequeue.QueueInfo, err error) {
	server.Lock()
	defer server.Unlock()
	if !server.FileQueueStatus || server.FileQueueObj == nil {
		err = fmt.Errorf("filequeue not start")
		return
	}
	return server.FileQueueObj.GetInfo(), nil
}
