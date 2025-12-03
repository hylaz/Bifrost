package xdb

import (
	"encoding/json"
	_ "github.com/brokercap/Bifrost/xdb/badger"
	_ "github.com/brokercap/Bifrost/xdb/bbolt"
	"github.com/brokercap/Bifrost/xdb/driver"
	_ "github.com/brokercap/Bifrost/xdb/leveldb"
	_ "github.com/brokercap/Bifrost/xdb/pebble"
	_ "github.com/brokercap/Bifrost/xdb/redis"
)

const DefaultPrefix = "xdb"

type Client struct {
	prefix string
	client driver.XdbDriver
}

func NewClient(name, uri string) (*Client, error) {
	client, err := driver.Open(name, uri)
	if err != nil {
		return nil, err
	}
	return &Client{
		client: client,
		prefix: DefaultPrefix,
	}, nil
}

func (client *Client) SetPrefix(prefix string) *Client {
	client.prefix = prefix
	return client
}

func (client *Client) GetKeyVal(table, key string, data interface{}) ([]byte, error) {
	myKey := []byte(client.prefix + ":" + table + ":" + key)
	s, err := client.client.GetKeyVal(myKey)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(s, data)
	if err != nil {
		return nil, err
	}
	return s, err
}

func (client *Client) PutKeyVal(table, key string, data interface{}) error {
	myKey := []byte(client.prefix + ":" + table + ":" + key)
	val, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = client.client.PutKeyVal(myKey, val)
	return err
}

func (client *Client) GetKeyValBytes(table, key string) ([]byte, error) {
	myKey := []byte(client.prefix + ":" + table + ":" + key)
	s, err := client.client.GetKeyVal(myKey)
	return s, err
}

func (client *Client) PutKeyValBytes(table, key string, val []byte) error {
	myKey := []byte(client.prefix + ":" + table + ":" + key)
	err := client.client.PutKeyVal(myKey, val)
	return err
}

func (client *Client) DelKeyVal(table, key string) error {
	myKey := []byte(client.prefix + ":" + table + ":" + key)
	return client.client.DelKeyVal(myKey)
}

func (client *Client) GetListByKeyPrefix(table, key string, data interface{}) ([]driver.ListValue, error) {
	prefix := client.prefix + ":" + table + ":"
	prefixLen := len(prefix)
	myKey := []byte(prefix + key)
	s, err := client.client.GetListByKeyPrefix(myKey)
	if err != nil {
		return s, err
	}

	var val = ""
	for k, v := range s {
		if data != nil {
			if val == "" {
				val = v.Value
			} else {
				val += "," + v.Value
			}
		}
		s[k].Key = s[k].Key[prefixLen:]
	}

	if data != nil {
		val = "[" + val + "]"
		err = json.Unmarshal([]byte(val), &data)
	}
	return s, err
}

func (client *Client) Close() error {
	return client.client.Close()
}
