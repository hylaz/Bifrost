package serverdo

import (
	"github.com/hprose/hprose-golang/rpc"
	"github.com/sirupsen/logrus"
)

var i int

func init() {
	i = 1
}

func Check(context *rpc.HTTPContext) (e error) {
	logrus.Println("Check success")
	return nil
}

func Insert(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	logrus.Println("Insert")
	logrus.Println("SchemaName:", SchemaName)
	logrus.Println("TableName:", TableName)
	logrus.Println(i, "data:", data)
	i++
	return nil
}

func Update(SchemaName string, TableName string, data []map[string]interface{}) (e error) {
	logrus.Println("Update")
	logrus.Println("SchemaName:", SchemaName)
	logrus.Println("TableName:", TableName)
	logrus.Println(i, "data:", data)
	i++
	return nil
}

func Delete(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	logrus.Println("Delete")
	logrus.Println("SchemaName:", SchemaName)
	logrus.Println("TableName:", TableName)
	logrus.Println(i, "data:", data)
	i++
	return nil
}

func Query(SchemaName string, TableName string, data interface{}) (e error) {
	logrus.Println(i, "Query", "SchemaName:", SchemaName, "TableName:", TableName, "data:", data)
	i++
	return nil
}

func Commit(SchemaName string, TableName string, data interface{}) (e error) {
	logrus.Println(i, "Commit", "SchemaName:", SchemaName, "TableName:", TableName, "data:", data)
	i++
	return nil
}
