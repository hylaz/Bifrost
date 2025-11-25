package controller

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/server/warning"
	"io"
	"strconv"
	"strings"
)

type WarningController struct {
	CommonController
}

type WarningParam struct {
	Type  string                 `json:"Type"`
	Param map[string]interface{} `json:"Param"`
	Id    string                 `json:"Id"`
}

func (c *WarningController) getParam() *WarningParam {
	body, err := io.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data WarningParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *WarningController) Index() {
	c.SetTitle("Warning Config List")
	c.SetData("WaringConfigList", warning.GetWarningConfigList())
	c.AddAdminTemplate("warning.config.list.html", "header.html", "footer.html")
}

func (c *WarningController) List() {
	c.SetJsonData(warning.GetWarningConfigList())
	c.StopServeJSON()
}

func (c *WarningController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.Type == "" || len(param.Param) == 0 {
		result.Msg = " Type and Param not empty!"
		return
	}
	id, err := warning.AddNewWarningConfig(warning.WaringConfig{Type: param.Type, Param: param.Param})
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: id}
	}
}

func (c *WarningController) Check() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.Type == "" || len(param.Param) == 0 {
		result.Msg = " Type and Param not empty!"
		return
	}
	err := warning.CheckWarngConfigBySendTest(warning.WaringConfig{Type: param.Type, Param: param.Param}, "it is test")
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	}
}

func (c *WarningController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	tmp := strings.Split(param.Id, "_")
	idString := tmp[len(tmp)-1]
	id, err := strconv.Atoi(idString)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	warning.DelWarningConfig(id)
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}
