package controller

import (
	"github.com/brokercap/Bifrost/server"
	"io"
	"time"
)

type BackupController struct {
	CommonController
}

// Export 导出配置
func (c *BackupController) Export() {
	b, err := server.GetSnapshotData()
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	c.SetOutputByUser()
	fileName := "bifrost_" + time.Now().Format("2006-01-02 15:04:05") + ".json"
	c.Ctx.ResponseWriter.Header().Add("Content-Type", "application/octet-stream")
	c.Ctx.ResponseWriter.Header().Add("content-disposition", "attachment; filename=\""+fileName+"\"")
	c.Ctx.ResponseWriter.Write(b)
}

// Import 导入配置
func (c *BackupController) Import() {
	c.Ctx.Request.ParseMultipartForm(32 << 20)
	file, _, err := c.Ctx.Request.FormFile("backup_file")
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	fileContent, err := io.ReadAll(file)
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	file.Close()
	server.DoRecoveryByBackupData(string(fileContent))
	c.SetJsonData(ResultDataStruct{Status: 1, Msg: "success", Data: nil})
}
