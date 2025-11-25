package controller

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/plugin/driver"
	ToServerStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"io"
)

type ToServerController struct {
	CommonController
}

type ToServerParam struct {
	ToServerKey string `json:"ToServerKey"`
	PluginName  string `json:"PluginName"`
	Notes       string `json:"Notes"`
	ConnUri     string `json:"ConnUri"`
	MaxConn     int    `json:"MaxConn"` // 最大连接数
	MinConn     int    `json:"MinConn"` // 最小连接数
}

func (c *ToServerController) getParam() *ToServerParam {
	body, err := io.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data ToServerParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *ToServerController) Index() {
	c.SetData("ToServerList", ToServerStorage.ToServerMap)
	c.SetData("Drivers", driver.Drivers())
	c.SetTitle("ToServer List")
	c.AddAdminTemplate("toserver.list.html", "header.html", "footer.html")
}

func (c *ToServerController) List() {
	c.SetData("ToServerList", ToServerStorage.ToServerMap)
	c.SetData("Drivers", driver.Drivers())
	c.StopServeJSON()
}

func (c *ToServerController) CheckUri() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "PluginName,connuri muest be not empty"
		return
	}
	err := driver.CheckUri(param.PluginName, &param.ConnUri)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

func (c *ToServerController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" || param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "toserverkey,PluginName,connuri muest be not empty"
		return
	}
	ToServerStorage.SetToServerInfo(
		param.ToServerKey,
		ToServerStorage.ToServer{
			PluginName: param.PluginName,
			ConnUri:    param.ConnUri,
			Notes:      param.Notes,
			MaxConn:    param.MaxConn,
			MinConn:    param.MinConn,
		})
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

// Update 更新数据
func (c *ToServerController) Update() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" || param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "toserverkey,PluginName,connuri muest be not empty"
		return
	}
	defer server.SaveDBConfigInfo()
	ToServerStorage.UpdateToServerInfo(
		param.ToServerKey,
		ToServerStorage.ToServer{
			PluginName: param.PluginName,
			ConnUri:    param.ConnUri,
			Notes:      param.Notes,
			MaxConn:    param.MaxConn,
			MinConn:    param.MinConn,
		})
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

func (c *ToServerController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" {
		result.Msg = "toserverkey muest be not empty"
		return
	}
	ToServerStorage.DelToServerInfo(param.ToServerKey)
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}
