package redis

import (
	"context"
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	"time"
)

type RedisDriver struct{}

var ctx = context.Background()

func (driver *RedisDriver) Open(uri string) (driver.XdbDriver, error) {
	return newConn(uri)
}

func newConn(uri string) (*RedisConn, error) {
	pwd, network, uri, database := getUriParam(uri)
	f := &RedisConn{
		pwd:      pwd,
		network:  network,
		database: database,
		Uri:      uri,
	}
	f.Connect()
	return f, nil
}

func getUriParam(uri string) (pwd string, network string, url string, database int) {
	i := strings.IndexAny(uri, "@")
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

type RedisConn struct {
	Uri      string
	pwd      string
	database int
	network  string
	status   string
	conn     redis.UniversalClient
	err      error
}

func (redisConn *RedisConn) Connect() error {
	if redisConn.database < 0 || redisConn.database > 16 {
		redisConn.err = fmt.Errorf("database must be in 0 and 16")
		return redisConn.err
	}
	if redisConn.network != "tcp" {
		redisConn.err = fmt.Errorf("network must be tcp")
		return redisConn.err
	}
	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(redisConn.Uri, ",", -1),
		Password: redisConn.pwd,
		DB:       redisConn.database,
		PoolSize: 4096,
	})

	_, redisConn.err = universalClient.Ping(ctx).Result()
	if redisConn.err != nil {
		redisConn.status = ""
		return redisConn.err
	}

	redisConn.conn = universalClient

	if redisConn.conn == nil {
		redisConn.err = fmt.Errorf("redis connect error")
		redisConn.status = ""
		return redisConn.err
	} else {
		redisConn.err = nil
		return redisConn.err
	}
}

func (redisConn *RedisConn) Close() error {
	if redisConn.conn != nil {
		redisConn.conn.Close()
	}
	redisConn.conn = nil
	return nil
}

func (redisConn *RedisConn) InitConn() {
	if redisConn.conn == nil {
		redisConn.Connect()
	}
}

func (redisConn *RedisConn) GetKeyVal(key []byte) ([]byte, error) {
	redisConn.InitConn()
	f := redisConn.conn.Get(ctx, string(key))
	s, err := f.Bytes()
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		redisConn.Close()
		return nil, err
	}
	return s, nil
}

func (redisConn *RedisConn) PutKeyVal(key []byte, val []byte) error {
	redisConn.InitConn()
	err := redisConn.conn.Set(ctx, string(key), string(val), time.Duration(0)).Err()
	if err != nil {
		redisConn.Close()
		return err
	}
	return nil
}

func (redisConn *RedisConn) DelKeyVal(key []byte) error {
	redisConn.InitConn()
	err := redisConn.conn.Del(ctx, string(key)).Err()
	if err != nil {
		if err.Error() != "redis: nil" {
			redisConn.Close()
			return err
		}
	}
	return nil
}

func (redisConn *RedisConn) GetListByKeyPrefix(key []byte) ([]driver.ListValue, error) {
	redisConn.InitConn()
	data := make([]driver.ListValue, 0)
	list, err := redisConn.conn.Keys(ctx, string(key)+"*").Result()
	if err != nil {
		redisConn.Close()
		return data, err
	}
	for _, vk := range list {
		val, err := redisConn.GetKeyVal([]byte(vk))
		if err != nil {
			return data, err
		}
		data = append(data,
			driver.ListValue{
				Key:   vk,
				Value: string(val),
			})
	}
	return data, nil
}
