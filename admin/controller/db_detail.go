package controller

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	toserver "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"strings"
)

func (c *DBController) Detail() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	dbInfo := server.GetDBObj(DbName)
	o := inputDriver.Open(dbInfo.InputType, inputDriver.InputInfo{ConnectUri: dbInfo.ConnectUri})
	DataBaseList, _ := o.GetSchemaList()
	DataBaseList = append(DataBaseList, "AllDataBases")

	c.SetData("DbName", DbName)
	c.SetData("DataBaseList", DataBaseList)
	c.SetData("ToServerList", toserver.GetToServerMap())
	c.SetData("ChannelList:", dbInfo.ListChannel())
	c.SetData("Title", DbName+" - Detail")
	c.AddAdminTemplate("db.detail.html", "header.html", "db.detail.table.add.html", "db.detail.history.add.html", "footer.html")
}

func (c *DBController) GetTableFields() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	TableName := c.Ctx.Request.Form.Get("TableName")

	SchemaName = tansferSchemaName(SchemaName)
	TableName = tansferTableName(TableName)

	dbInfo := server.GetDBObj(DbName)
	o := inputDriver.Open(dbInfo.InputType, inputDriver.InputInfo{ConnectUri: dbInfo.ConnectUri})
	TableFieldsList, _ := o.GetSchemaTableFieldList(SchemaName, TableName)
	c.SetJsonData(TableFieldsList)
	c.StopServeJSON()
}

func (c *DBController) TableList() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	DBObj := server.GetDBObj(DbName)
	o := inputDriver.Open(DBObj.InputType, inputDriver.InputInfo{ConnectUri: DBObj.ConnectUri})
	type ResultType struct {
		TableName   string
		ChannelName string
		AddStatus   bool
		TableType   string
		IgnoreTable string
		DoTable     string
	}
	var data []ResultType
	data = make([]ResultType, 0)
	TableList, _ := o.GetSchemaTableList(SchemaName)
	TableList = append(TableList, inputDriver.TableList{TableName: "AllTables", TableType: "LIKE"})
	var schemaName0, tableName0 string
	schemaName0 = tansferSchemaName(SchemaName)

	for _, tableInfo := range TableList {
		tableName := tableInfo.TableName
		tableType := tableInfo.TableType
		tableName0 = tansferTableName(tableName)
		t := DBObj.GetTable(schemaName0, tableName0)
		if t == nil {
			data = append(data, ResultType{TableName: tableName, ChannelName: "", AddStatus: false, TableType: tableType})
		} else {
			t2 := DBObj.GetChannel(t.ChannelKey)
			if t2 == nil {
				data = append(data, ResultType{TableName: tableName, ChannelName: "", AddStatus: false, TableType: tableType})
			} else {
				data = append(data, ResultType{TableName: tableName, ChannelName: t2.Name, AddStatus: true, TableType: tableType, IgnoreTable: t.IgnoreTable, DoTable: t.DoTable})
			}
		}
	}
	// 将 带 * 等模糊匹配的表配置 往 list 追加
	// 只有是当前数据库的数据才能被追加进去
	if schemaName0 != "*" {
		for k, v := range DBObj.GetTables() {
			schema_name1, tableName1 := server.GetSchemaAndTableBySplit(k)
			if tableName1 == "*" {
				continue
			}
			if schema_name1 != schemaName0 {
				continue
			}
			if strings.Index(v.Name, "*") < 0 {
				continue
			}
			t2 := DBObj.GetChannel(v.ChannelKey)
			data = append(data, ResultType{TableName: v.Name, ChannelName: t2.Name, AddStatus: true, TableType: "LIKE", IgnoreTable: v.IgnoreTable})
		}
	}
	c.SetJsonData(data)
	c.StopServeJSON()
}
