package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/hprose/hprose-golang/rpc"
)

const Version = "v1.6.0"
const BifrostVersion = "v1.6.0"

func init() {
	pluginDriver.Register("hprose", NewHproseConn, Version, BifrostVersion)
}

type Stub struct {
	Check  func() error
	Insert func(SchemaName string, TableName string, data map[string]interface{}) (e error)
	Update func(SchemaName string, TableName string, data []map[string]interface{}) (e error)
	Delete func(SchemaName string, TableName string, data map[string]interface{}) (e error)
	Query  func(SchemaName string, TableName string, sql string) (e error)
}

var stub *Stub

type HproseConn struct {
	pluginDriver.PluginDriverInterface
	Uri           string
	status        string
	httpClient    *rpc.HTTPClient
	tcpClient     *rpc.TCPClient
	defaultClient rpc.Client
	clientType    string
	err           error
}

func NewHproseConn() pluginDriver.Driver {
	f := &HproseConn{
		status: "close",
	}
	return f
}

func (hproseConn *HproseConn) SetOption(uri *string, param map[string]interface{}) {
	hproseConn.Uri = *uri
	return
}

func (hproseConn *HproseConn) SetParam(p interface{}) (interface{}, error) {
	return nil, nil
}

func (hproseConn *HproseConn) Open() error {
	hproseConn.Connect()
	return nil
}

func (hproseConn *HproseConn) GetUriExample() string {
	return "http://127.0.0.1:61613 or tcp4://127.0.0.1:4321/"
}

func (hproseConn *HproseConn) CheckUri() error {
	hproseConn.Connect()
	if hproseConn.status != "running" {
		return fmt.Errorf("not connect")
	}
	err := stub.Check()
	switch hproseConn.clientType {
	case "tcp":
		hproseConn.tcpClient.Close()
		break
	case "http":
		hproseConn.httpClient.Close()
		break
	default:
		hproseConn.defaultClient.Close()
		break
	}
	return err
}

func checkUriType(uri string) string {
	if uri[0:3] == "tcp" {
		return "tcp"
	}
	if uri[0:4] == "http" {
		return "http"
	}
	return ""
}

func (hproseConn *HproseConn) Connect() bool {
	hproseConn.clientType = checkUriType(hproseConn.Uri)
	switch hproseConn.clientType {
	case "tcp":
		hproseConn.tcpClient = rpc.NewTCPClient(hproseConn.Uri)
		hproseConn.tcpClient.UseService(&stub)
		break
	case "http":
		hproseConn.httpClient = rpc.NewHTTPClient(hproseConn.Uri)
		hproseConn.httpClient.UseService(&stub)
		break
	default:
		hproseConn.defaultClient = rpc.NewClient(hproseConn.Uri)
		hproseConn.defaultClient.UseService(&stub)
		break
	}
	hproseConn.status = "running"
	return true
}

func (hproseConn *HproseConn) ReConnect() bool {
	hproseConn.Connect()
	return true
}

func (hproseConn *HproseConn) Close() bool {
	hproseConn.clientType = checkUriType(hproseConn.Uri)
	switch hproseConn.clientType {
	case "tcp":
		hproseConn.tcpClient.Close()
		break
	case "http":
		hproseConn.httpClient.Close()
		break
	default:
		hproseConn.defaultClient.Close()
		break
	}
	return true
}

func (hproseConn *HproseConn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Insert(data.SchemaName, data.TableName, data.Rows[0])
	if err != nil {
		hproseConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (hproseConn *HproseConn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Update(data.SchemaName, data.TableName, data.Rows)
	if err != nil {
		hproseConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (hproseConn *HproseConn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Delete(data.SchemaName, data.TableName, data.Rows[0])
	if err != nil {
		hproseConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (hproseConn *HproseConn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Query(data.SchemaName, data.TableName, data.Query)
	if err != nil {
		hproseConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (hproseConn *HproseConn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Query(data.SchemaName, data.TableName, data.Query)
	if err != nil {
		hproseConn.err = err
		return nil, data, err
	}
	return data, nil, nil
}
