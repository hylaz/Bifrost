package src

import (
	"context"
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	elastic "github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.8.5-beta"
const BIFROST_VERION = "v1.8.5"

func init() {
	pluginDriver.Register("Elasticsearch", NewElasticsearchConn, VERSION, BIFROST_VERION)
}

type ElasticsearchConn struct {
	pluginDriver.PluginDriverInterface
	Uri    string
	status string
	client *elastic.Client

	err error
	p   *PluginParam

	esServerInfo *EsServer
}

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType // commit 提交的数据列表，Data 每 BatchSize 数据量划分为一个最后提交的commit
}

type PluginParam struct {
	EsIndexName          string          `json: "EsIndexName"`
	PrimaryKey           string          `json: "PrimaryKey"`
	Mapping              string          `json: "Mapping"`
	primaryKeys          []string        `json: "primaryKeys"`
	hadMapping           map[string]bool `json: "hadMapping"`
	BifrostMustBeSuccess bool            `json: "BifrostMustBeSuccess"` // bifrost server 保留,数据是否能丢
	BatchSize            int             `json: "BatchSize"`
	Data                 *TableDataStruct
	SkipBinlogData       *pluginDriver.PluginDataType // 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}

type EsServer struct {
	User       string
	Password   string
	Urls       []string
	Sniff      bool
	Timeout    int
	RetryCount int
}

func NewElasticsearchConn() pluginDriver.Driver {
	f := &ElasticsearchConn{status: "close", err: fmt.Errorf("close")}
	return f
}

func (conn *ElasticsearchConn) SetOption(uri *string, param map[string]interface{}) {
	conn.Uri = *uri
	return
}

func (conn *ElasticsearchConn) Open() error {
	conn.esServerInfo = conn.getUriParam(conn.Uri)
	conn.Connect()
	return nil
}

func (conn *ElasticsearchConn) GetUriExample() string {
	return "http://localhost:9200?user=root&password=rootroot"
}

func (conn *ElasticsearchConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err = json.Unmarshal(s, &param)
	if err != nil {
		return nil, err
	}
	if param.EsIndexName == "" {
		return nil, fmt.Errorf("EsIndexName can't be empty")
	}
	param.primaryKeys = strings.Split(param.PrimaryKey, ",")
	param.hadMapping = map[string]bool{}
	param.Data = NewTableData()
	if param.BatchSize == 0 {
		param.BatchSize = 100
	}
	return &param, nil
}

func (conn *ElasticsearchConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		conn.p = p.(*PluginParam)
		return p, nil
	default:
		param, _ := conn.GetParam(p)
		conn.p = param
		return param, nil
	}
}

func (conn *ElasticsearchConn) CheckUri() error {
	var err error
	conn.Connect()
	if conn.err != nil {
		return conn.err
	}
	_, err = conn.GetVersion()
	return err
}

func (conn *ElasticsearchConn) getUriParam(uri string) (EsServerInfo *EsServer) {
	EsServerInfo = &EsServer{}
	EsServerInfo.Urls = make([]string, 0)
	for _, httpUrl := range strings.Split(uri, ",") {
		if httpUrl == "" {
			continue
		}
		urlInfo, _ := url.Parse(httpUrl)
		auths := urlInfo.Query()
		if len(auths["user"]) > 0 {
			EsServerInfo.User = auths["user"][0]
		}
		if len(auths["password"]) > 0 {
			EsServerInfo.Password = auths["password"][0]
		}
		if len(auths["sniff"]) > 0 {
			if auths["sniff"][0] == "true" {
				EsServerInfo.Sniff = true
			}
		}
		if len(auths["timeout"]) > 0 {
			n, _ := strconv.Atoi(auths["timeout"][0])
			if n > 0 {
				EsServerInfo.Timeout = n
			}
		}
		if len(auths["retryCount"]) > 0 {
			n, _ := strconv.Atoi(auths["retryCount"][0])
			if n > 0 {
				EsServerInfo.RetryCount = n
			}
		}
		index := strings.Index(httpUrl, "?")
		if index > 0 {
			EsServerInfo.Urls = append(EsServerInfo.Urls, httpUrl[0:index])
		} else {
			EsServerInfo.Urls = append(EsServerInfo.Urls, httpUrl)
		}
	}
	if EsServerInfo.Timeout == 0 {
		EsServerInfo.Timeout = 10
	}
	if EsServerInfo.RetryCount == 0 {
		EsServerInfo.RetryCount = 3
	}
	return
}

func (conn *ElasticsearchConn) Connect() bool {

	// This.Uri   http://127.0.0.1:9200?user=root&password=rootroot
	EsServerInfo := conn.getUriParam(conn.Uri)
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(EsServerInfo.Urls...),
		elastic.SetSniff(EsServerInfo.Sniff),
	}
	if EsServerInfo.User != "" {
		options = append(options, elastic.SetBasicAuth(EsServerInfo.User, EsServerInfo.Password))
	}

	options = append(options, elastic.SetHttpClient(&http.Client{
		Timeout: time.Duration(EsServerInfo.Timeout) * time.Second,
	}))

	client, err := elastic.NewClient(options...)
	if err != nil {
		conn.err = err
		return false
	}
	conn.esServerInfo = EsServerInfo
	conn.client = client
	conn.err = nil
	conn.status = "running"
	return true
}

func (conn *ElasticsearchConn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			conn.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	conn.Close()
	conn.Connect()
	return true
}

func (conn *ElasticsearchConn) Close() bool {
	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
	}()
	conn.status = "close"
	conn.client = nil
	conn.err = fmt.Errorf("close")
	return true
}

func (conn *ElasticsearchConn) GetVersion() (Version string, err error) {

	if conn.err != nil {
		conn.Connect()
	}
	EsServerInfo := conn.getUriParam(conn.Uri)
	Version, err = conn.client.ElasticsearchVersion(EsServerInfo.Urls[0])
	return
}

func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType, 0)
	CommitData = append(CommitData, nil)
	return &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: CommitData,
	}
}

// 假如没有配置指定 PrimaryKey (es 中的文档ID) 的时候，将 原表中的 Pri 主键当作 es 的文档ID
func (conn *ElasticsearchConn) initPrimaryKeys(data *pluginDriver.PluginDataType) {
	if conn.p.PrimaryKey == "" {
		conn.p.primaryKeys = data.Pri
	}
}

func (conn *ElasticsearchConn) doCreateMapping() {
	EsIndexName := conn.p.EsIndexName
	if conn.p.Mapping == "" {
		conn.p.hadMapping[EsIndexName] = true
		return
	}
	if _, ok := conn.p.hadMapping[EsIndexName]; !ok {
		resp, err := conn.client.GetMapping().Index(EsIndexName).Do(context.Background())

		if err == nil && resp != nil {
			if _, ok := resp[EsIndexName]; ok { // hadMapping
				conn.p.hadMapping[EsIndexName] = true
				return
			}
		}
		var mapping map[string]interface{}
		err = json.Unmarshal([]byte(conn.p.Mapping), &mapping)
		if err == nil {
			conn.client.PutMapping().Index(EsIndexName).BodyJson(mapping).Do(context.Background())
		} else {
			logrus.Printf("output[elasticsearch] doCreateMapping json.Unmarshal err: %s , mapping:%s", err.Error(), mapping)
		}
		conn.p.hadMapping[EsIndexName] = true
	}
}

func (conn *ElasticsearchConn) doCommit(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType, err error) {

	if len(list) > 0 {
		conn.p.EsIndexName = strings.ToLower(fmt.Sprint(pluginDriver.TransfeResult(conn.p.EsIndexName, list[0], 0)))
	}

	//This.doCreateMapping()
	errData, err = conn.commitNormal(list, n)
	return
}

func (conn *ElasticsearchConn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(string(debug.Stack()))
			conn.err = e
		}
	}()
	if conn.err != nil {
		conn.ReConnect()
	}
	if conn.err != nil {
		logrus.Println(" This.Err:", conn.err)
		return nil, nil, conn.err
	}
	if conn.err != nil {
		logrus.Println("This.err:", conn.err)
	}
	n := len(conn.p.Data.Data)
	if n == 0 {
		return nil, nil, nil
	}

	if n > conn.p.BatchSize {
		n = conn.p.BatchSize
	}
	list := conn.p.Data.Data[:n]

	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, dataList := range dataMap {
		ErrData, e = conn.doCommit(dataList, len(dataList))
		// 假如数据不能丢，才需要 判断 是否有err，如果可以丢，直接错过数据
		if e != nil {
			conn.err = e
			if conn.p.BifrostMustBeSuccess {
				return nil, ErrData, conn.err
			}
			if conn.CheckDataSkip(ErrData) {
				continue
			}
		}
	}
	conn.err = e
	var binlogEvent *pluginDriver.PluginDataType
	if len(conn.p.Data.Data) <= int(conn.p.BatchSize) {
		binlogEvent = conn.p.Data.CommitData[0]
		conn.p.Data = NewTableData()
	} else {
		conn.p.Data.Data = conn.p.Data.Data[n:]
		if len(conn.p.Data.CommitData) > 0 {
			binlogEvent = conn.p.Data.CommitData[0]
			conn.p.Data.CommitData = conn.p.Data.CommitData[1:]
		}
	}
	conn.p.SkipBinlogData = nil
	return binlogEvent, nil, nil
}

// 将数据放到 list 里,假如满足条件，则合并提交数据到es里
func (conn *ElasticsearchConn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	var n int
	if retry == false {
		conn.p.Data.Data = append(conn.p.Data.Data, data)
	}
	n = len(conn.p.Data.Data)

	if conn.p.BatchSize <= n {
		return conn.AutoCommit()
	}
	return nil, nil, nil
}

func (conn *ElasticsearchConn) Insert(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	conn.initPrimaryKeys(data)
	if len(conn.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return conn.sendToCacheList(data, retry)
}

func (conn *ElasticsearchConn) Update(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	conn.initPrimaryKeys(data)
	if len(conn.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return conn.sendToCacheList(data, retry)
}

func (conn *ElasticsearchConn) Del(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	conn.initPrimaryKeys(data)
	if len(conn.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return conn.sendToCacheList(data, retry)
}

func (conn *ElasticsearchConn) Query(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (conn *ElasticsearchConn) Commit(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	n := len(conn.p.Data.Data)
	if n == 0 {
		return data, nil, nil
	}

	n0 := n / conn.p.BatchSize
	if len(conn.p.Data.CommitData)-1 < n0 {
		conn.p.Data.CommitData = append(conn.p.Data.CommitData, data)
	} else {
		conn.p.Data.CommitData[n0] = data
	}
	return nil, nil, nil
}

func (conn *ElasticsearchConn) TimeOutCommit() (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.AutoCommit()
}

// 设置跳过的位点
func (conn *ElasticsearchConn) Skip(SkipData *pluginDriver.PluginDataType) error {
	conn.p.SkipBinlogData = SkipData
	return nil
}

func (conn *ElasticsearchConn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if conn.p.SkipBinlogData != nil && conn.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && conn.p.SkipBinlogData.BinlogPosition == data.BinlogPosition {
		if conn.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && conn.p.SkipBinlogData.BinlogPosition >= data.BinlogPosition {
			return true
		}
		if conn.p.SkipBinlogData.BinlogFileNum > data.BinlogFileNum {
			return true
		}
	}
	return false
}
