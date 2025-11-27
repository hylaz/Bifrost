package history

import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/server"
	"github.com/sirupsen/logrus"
)

func CheckWhere(dbName, SchemaName, TableName, Where string) error {
	if Where == "" {
		return nil
	}
	dbObj := server.GetDBObj(dbName)
	if dbObj == nil {
		return fmt.Errorf("%s not exist", dbName)
	}
	return checkDbWhere(dbObj.ConnectUri, SchemaName, TableName, Where)
}

func checkDbWhere(Uri string, SchemaName string, TableName string, Where string) error {
	db := DBConnect(Uri)
	if db != nil {
		defer db.Close()
	}
	sql := "SELECT * FROM `" + SchemaName + "`.`" + TableName + "` WHERE " + Where + " LIMIT 1"
	rows, err := db.Query(sql, []driver.Value{})
	if err != nil {
		logrus.Println("CheckWhere:", err, "sql:", sql)
		return err
	}
	rows.Close()
	return nil
}
