package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"net/http"
	"strings"
	"time"
)

const Version = "v1.8.5"
const BifrostVersion = "v1.8.5"

func init() {
	pluginDriver.Register("http", NewHttpConn, Version, BifrostVersion)
}

type HttpConn struct {
	pluginDriver.PluginDriverInterface
	uri    string
	url    string
	user   string
	pwd    string
	status string
	err    error
	p      *PluginParam
}

type HttpContentType string

const (
	JsonRaw HttpContentType = "application/json-raw"
)

type PluginParam struct {
	Timeout     int
	ContentType HttpContentType
	FilterQuery bool
}

func NewHttpConn() pluginDriver.Driver {
	f := &HttpConn{
		status: "close",
	}
	return f
}

func (httpConn *HttpConn) SetOption(uri *string, param map[string]interface{}) {
	httpConn.uri = *uri
	return
}

func (httpConn *HttpConn) Open() error {
	httpConn.user, httpConn.pwd, httpConn.url = GetUriParam(httpConn.uri)
	return nil
}

func (httpConn *HttpConn) GetUriExample() string {
	return "user:pwd@http://a.Bifrist.com?bifrost_api=ok ; http://a.Bifrist.com?bifrost_api=ok"
}

func (httpConn *HttpConn) CheckUri() error {
	user, pwd, url := GetUriParam(httpConn.uri)
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if user != "" {
		req.SetBasicAuth(user, pwd)
	}
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("http code:%d", resp.StatusCode)
}

func GetUriParam(uri string) (string, string, string) {
	i := strings.IndexAny(uri, "@")
	var user, pwd = "", ""
	var url string
	if i > 0 {
		t := uri[0:i]
		j := strings.IndexAny(t, ":")
		if j > 0 {
			user = t[0:j]
			pwd = t[j+1:]
		} else {
			user = t
		}
		url = uri[i+1:]
	} else {
		url = uri
	}
	return user, pwd, url
}

func (httpConn *HttpConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err = json.Unmarshal(s, &param)
	if err != nil {
		return nil, err
	}
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.ContentType != JsonRaw {
		return nil, fmt.Errorf("only support application/json(raw)")
	}
	httpConn.p = &param
	return &param, nil
}

func (httpConn *HttpConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		httpConn.p = p.(*PluginParam)
		return p, nil
	default:
		return httpConn.GetParam(p)
	}
}

func (httpConn *HttpConn) httpPost(data *pluginDriver.PluginDataType) error {
	var req *http.Request
	var client *http.Client
	var err error
	switch httpConn.p.ContentType {
	case JsonRaw:
		c, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body := strings.NewReader("\n" + string(c))
		req, err = http.NewRequest("POST", httpConn.url, body)
		req.Header.Set("Content-Type", "application/json")
		break
	default:
		return fmt.Errorf("only support application/json(raw)")
	}

	if err != nil {
		return err
	}

	client = &http.Client{Timeout: time.Duration(httpConn.p.Timeout) * time.Second}
	if httpConn.user != "" {
		req.SetBasicAuth(httpConn.user, httpConn.pwd)
	}

	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 || (resp.StatusCode > 200 && resp.StatusCode < 300) {
		return nil
	}
	return fmt.Errorf("http code:%d", resp.StatusCode)
}

func (httpConn *HttpConn) Close() bool {
	return true
}

func (httpConn *HttpConn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := httpConn.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (httpConn *HttpConn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := httpConn.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (httpConn *HttpConn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := httpConn.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (httpConn *HttpConn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {

	if httpConn.p.FilterQuery {
		return data, nil, nil
	}

	err := httpConn.httpPost(data)
	if err != nil {
		httpConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (httpConn *HttpConn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if httpConn.p.FilterQuery {
		return data, nil, nil
	}
	err := httpConn.httpPost(data)
	if err != nil {
		httpConn.err = err
		return nil, data, err
	}
	return data, nil, nil
}
