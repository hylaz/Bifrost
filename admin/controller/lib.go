package controller

import (
	"github.com/brokercap/Bifrost/config"
)

func AdminTemplatePath(fileName string) string {
	return config.BifrostAdminTemplateDir + fileName
}

func PluginTemplatePath(fileName string) string {
	return config.BifrostPluginTemplateDir + fileName
}

type ResultDataStruct struct {
	Status int8        `json:"status"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data"`
}
