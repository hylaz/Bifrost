package src

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const VERSION = "v1.7.4"
const BIFROST_VERION = "v1.7.4"

func init() {
	driver.Register("redis", NewRedisConn, VERSION, BIFROST_VERION)
}

var ctx = context.Background()

type RedisConn struct {
	driver.PluginDriverInterface
	Uri    *string
	status string
	conn   redis.UniversalClient
	err    error
	p      *PluginParam
}

type PluginParam struct {
	KeyConfig          string
	Expir              int
	DataType           string
	ValConfig          string
	Type               string
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func NewRedisConn() driver.Driver {
	f := &RedisConn{
		status: "close",
	}
	return f
}

func (redisConn *RedisConn) SetOption(uri *string, param map[string]interface{}) {
	redisConn.Uri = uri
	return
}

func (redisConn *RedisConn) Open() error {
	redisConn.Connect()
	return nil
}

func (redisConn *RedisConn) GetUriExample() string {
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379 or pwd@tcp(127.0.0.1:6379,127.0.0.1:6380)/0 or 127.0.0.1:6379,127.0.0.1:6380"
}

func (redisConn *RedisConn) CheckUri() error {
	redisConn.Connect()
	if redisConn.err != nil {
		return redisConn.err
	}
	redisConn.Close()
	return nil
}

func GetUriParam(uri string) (pwd string, network string, url string, database int) {
	i := strings.LastIndex(uri, "@")
	pwd = ""
	if i > 0 {
		pwd = uri[0:i]
		url = uri[i+1:]
	} else {
		url = uri
	}
	i = strings.IndexAny(url, "/")
	if i > 0 {
		databaseString := url[i+1:]
		intv, err := strconv.Atoi(databaseString)
		if err != nil {
			database = -1
		}
		database = intv
		url = url[0:i]
	} else {
		database = 0
	}
	i = strings.IndexAny(url, "(")
	if i > 0 {
		network = url[0:i]
		url = url[i+1 : len(url)-1]
	} else {
		network = "tcp"
	}
	return
}

func (redisConn *RedisConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err = json.Unmarshal(s, &param)
	if err != nil {
		return nil, err
	}
	redisConn.p = &param
	return &param, nil
}

func (redisConn *RedisConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		redisConn.p = p.(*PluginParam)
		return p, nil
	default:
		return redisConn.GetParam(p)
	}
}

func (redisConn *RedisConn) Connect() bool {
	pwd, network, uri, database := GetUriParam(*redisConn.Uri)
	if database < 0 {
		redisConn.err = fmt.Errorf("database must be in 0 and 16")
		return false
	}
	if network != "tcp" {
		redisConn.err = fmt.Errorf("network must be tcp")
		return false
	}

	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(uri, ",", -1),
		Password: pwd,
		DB:       database,
		PoolSize: 4096,
	})

	_, redisConn.err = universalClient.Ping(ctx).Result()
	if redisConn.err != nil {
		redisConn.status = ""
		return false
	}
	redisConn.conn = universalClient
	if redisConn.conn == nil {
		redisConn.status = ""
		redisConn.err = errors.New("connect error")
		return false
	} else {
		redisConn.status = "running"
		redisConn.err = nil
		return true
	}
}

func (redisConn *RedisConn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			redisConn.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	if redisConn.conn != nil {
		redisConn.conn.Close()
	}
	redisConn.Connect()
	return true
}

func (redisConn *RedisConn) Close() bool {
	if redisConn.conn != nil {
		redisConn.conn.Close()
	}
	return true
}

func (redisConn *RedisConn) getKeyVal(data *driver.PluginDataType, index int) string {
	return fmt.Sprint(driver.TransfeResult(redisConn.p.KeyConfig, data, index))
}

func (redisConn *RedisConn) getVal(data *driver.PluginDataType, index int) string {
	return fmt.Sprint(driver.TransfeResult(redisConn.p.ValConfig, data, index))
}

func (redisConn *RedisConn) Insert(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	return redisConn.Update(data, retry)
}

func (redisConn *RedisConn) Update(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if redisConn.err != nil {
		redisConn.ReConnect()
	}
	index := len(data.Rows) - 1
	Key := redisConn.getKeyVal(data, index)
	var err error
	switch redisConn.p.Type {
	case "set":
		if redisConn.p.ValConfig != "" {
			err = redisConn.conn.Set(ctx, Key, redisConn.getVal(data, index), time.Duration(redisConn.p.Expir)*time.Second).Err()
		} else {
			vbyte, _ := json.Marshal(data.Rows[index])
			err = redisConn.conn.Set(ctx, Key, string(vbyte), time.Duration(redisConn.p.Expir)*time.Second).Err()
		}
		break
	case "list":
		return redisConn.SendToList(Key, data)
		break
	default:
		err = fmt.Errorf(redisConn.p.Type + " not in(set,list)")
		break
	}

	if err != nil {
		redisConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (redisConn *RedisConn) Del(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if redisConn.err != nil {
		redisConn.ReConnect()
	}

	Key := redisConn.getKeyVal(data, 0)
	var err error
	switch redisConn.p.Type {
	case "set":
		err = redisConn.conn.Del(ctx, Key).Err()
		break
	case "list":
		return redisConn.SendToList(Key, data)
		break
	default:
		err = fmt.Errorf(redisConn.p.Type + " not in(set,list)")
	}
	if err != nil {
		redisConn.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (redisConn *RedisConn) SendToList(Key string, data *driver.PluginDataType) (*driver.PluginDataType, *driver.PluginDataType, error) {
	var Val string
	var err error
	if redisConn.p.ValConfig != "" {
		Val = redisConn.getVal(data, 0)
	} else {
		c, err := json.Marshal(data)
		if err != nil {
			return nil, data, err
		}
		Val = string(c)
	}
	err = redisConn.conn.LPush(ctx, Key, Val).Err()

	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (redisConn *RedisConn) Query(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if redisConn.p.BifrostFilterQuery {
		return nil, nil, nil
	}
	if redisConn.p.Type == "list" {
		Key := redisConn.getKeyVal(data, 0)
		return redisConn.SendToList(Key, data)
	}
	return nil, nil, nil
}

func (redisConn *RedisConn) Commit(data *driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	if redisConn.p.BifrostFilterQuery {
		return data, nil, nil
	}
	if redisConn.p.Type == "list" {
		Key := redisConn.getKeyVal(data, 0)
		LastSuccessCommitData, ErrData, err = redisConn.SendToList(Key, data)
		if err != nil {
			return
		}
	}
	return data, nil, nil
}
