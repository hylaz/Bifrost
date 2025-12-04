package src

import (
	"context"
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"runtime/debug"
	"strings"
)

const Version = "v1.6.0"
const BifrostVersion = "v1.6.0"

func init() {
	pluginDriver.Register("MongoDB", NewMongoConn, Version, BifrostVersion)
}

type MongoConn struct {
	pluginDriver.PluginDriverInterface
	Uri    string
	status string
	client *mongo.Client
	err    error
	p      *PluginParam
}

type PluginParam struct {
	SchemaName  string
	TableName   string
	PrimaryKey  string
	primaryKeys []string
	hadIndexMap map[string]bool
	indexName   string
}

func NewMongoConn() pluginDriver.Driver {
	f := &MongoConn{status: "close", err: fmt.Errorf("close")}
	return f
}

func (mongoConn *MongoConn) SetOption(uri *string, param map[string]interface{}) {
	mongoConn.Uri = *uri
	return
}

func (mongoConn *MongoConn) Open() error {
	mongoConn.Connect()
	return nil
}

func (mongoConn *MongoConn) GetUriExample() string {
	return "[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]"
}

func (mongoConn *MongoConn) CheckUri() error {
	mongoConn.Connect()
	if mongoConn.status == "running" {
		mongoConn.Close()
		return nil
	} else {
		return mongoConn.err
	}
}

func (mongoConn *MongoConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err = json.Unmarshal(s, &param)
	if err != nil {
		return nil, err
	}

	if param.SchemaName == "" || param.TableName == "" {
		return nil, fmt.Errorf("SchemaName,TableName can't be empty")
	}
	param.indexName = "bifrost_unique_index"
	param.primaryKeys = strings.Split(param.PrimaryKey, ",")
	param.hadIndexMap = make(map[string]bool, 0)
	mongoConn.p = &param
	return &param, nil
}

func (mongoConn *MongoConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		mongoConn.p = p.(*PluginParam)
		return p, nil
	default:
		return mongoConn.GetParam(p)
	}
}

func (mongoConn *MongoConn) Connect() bool {
	var err error
	opt := options.Client().ApplyURI(mongoConn.Uri)
	mongoConn.client, err = mongo.Connect(context.Background(), opt)
	if err != nil {
		mongoConn.err = err
		mongoConn.status = "close"
		return false
	}
	mongoConn.err = nil
	mongoConn.status = "running"
	return true
}

func (mongoConn *MongoConn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			mongoConn.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	mongoConn.Close()
	mongoConn.Connect()
	return true
}

func (mongoConn *MongoConn) Close() bool {

	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		if mongoConn.client != nil {
			mongoConn.client.Disconnect(context.Background())
		}
	}()

	mongoConn.status = "close"
	mongoConn.client = nil
	mongoConn.err = fmt.Errorf("close")
	return true
}

func (mongoConn *MongoConn) initPrimaryKeys(data *pluginDriver.PluginDataType) {
	if mongoConn.p.PrimaryKey == "" {
		mongoConn.p.primaryKeys = data.Pri
	}
}

func (mongoConn *MongoConn) createIndex(c *mongo.Collection) {

	indexTableKey := c.Database().Name() + "#" + c.Name()
	if _, ok := mongoConn.p.hadIndexMap[indexTableKey]; !ok {
		keys := make(bson.D, 0, len(mongoConn.p.primaryKeys))
		for _, key := range mongoConn.p.primaryKeys {
			keys = append(keys, bson.E{Key: key, Value: 1}) // 1 表示升序
		}

		mod := mongo.IndexModel{
			Keys:    keys,
			Options: options.Index().SetName(mongoConn.p.indexName).SetUnique(true),
		}
		c.Indexes().CreateOne(context.Background(), mod)
	}
}

func (mongoConn *MongoConn) Insert(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	if mongoConn.err != nil {
		mongoConn.Connect()
	}
	if mongoConn.err != nil {
		return nil, data, mongoConn.err
	}
	mongoConn.initPrimaryKeys(data)
	if len(mongoConn.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}
	n := len(data.Rows) - 1
	SchemaName := fmt.Sprint(pluginDriver.TransfeResult(mongoConn.p.SchemaName, data, n))
	TableName := fmt.Sprint(pluginDriver.TransfeResult(mongoConn.p.TableName, data, n))
	defer func() {
		if err := recover(); err != nil {
			LastSuccessCommitData = nil
			e = fmt.Errorf(string(debug.Stack()))
			mongoConn.err = e
			logrus.Println(e)
			return
		}
	}()
	c := mongoConn.client.Database(SchemaName).Collection(TableName)
	k := make(bson.M, 1)
	for _, key := range mongoConn.p.primaryKeys {
		if _, ok := data.Rows[n][key]; ok {
			k[key] = data.Rows[n][key]
		} else {
			return nil, data, fmt.Errorf("key:" + key + " no exsit")
		}
	}
	opts := options.Update().SetUpsert(true)
	_, err := c.UpdateOne(context.Background(), k, data.Rows[n], opts)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (mongoConn *MongoConn) Update(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return mongoConn.Insert(data, retry)
}

func (mongoConn *MongoConn) Del(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	if mongoConn.err != nil {
		mongoConn.Connect()
	}
	if mongoConn.err != nil {
		return nil, data, mongoConn.err
	}
	mongoConn.initPrimaryKeys(data)
	if len(mongoConn.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}
	defer func() {
		if err := recover(); err != nil {
			LastSuccessCommitData = nil
			e = fmt.Errorf(string(debug.Stack()))
			mongoConn.err = e
			logrus.Println(string(debug.Stack()))
			return
		}
	}()
	SchemaName := fmt.Sprint(pluginDriver.TransfeResult(mongoConn.p.SchemaName, data, 0))
	TableName := fmt.Sprint(pluginDriver.TransfeResult(mongoConn.p.TableName, data, 0))
	c := mongoConn.client.Database(SchemaName).Collection(TableName)
	k := make(bson.M, 1)
	for _, key := range mongoConn.p.primaryKeys {
		if _, ok := data.Rows[0][key]; ok {
			k[key] = data.Rows[0][key]
		} else {
			return nil, data, fmt.Errorf("key:" + key + " no exsit")
		}
	}
	_, err := c.DeleteOne(context.Background(), k)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (mongoConn *MongoConn) Query(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return data, nil, nil
}

func (mongoConn *MongoConn) Commit(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return data, nil, nil
}
