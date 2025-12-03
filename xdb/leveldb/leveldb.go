package leveldb

import (
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"os"
	"strings"
)

type LeveldbDriver struct{}

func (leveldbDriver *LeveldbDriver) Open(path string) (driver.XdbDriver, error) {
	return newConn(path)
}

func newConn(path string) (*LeveldbConn, error) {
	if path == "" {
		return nil, fmt.Errorf("path error")
	}
	f := &LeveldbConn{
		path: path,
	}
	err := f.connect()
	if err != nil {
		return nil, err
	}
	return f, nil
}

type LeveldbConn struct {
	path    string
	err     error
	levelDB *leveldb.DB
}

func (leveldbConn *LeveldbConn) connect() error {
	os.MkdirAll(leveldbConn.path, 0755)
	leveldbConn.levelDB, leveldbConn.err = leveldb.OpenFile(leveldbConn.path, nil)
	return leveldbConn.err
}

func (leveldbConn *LeveldbConn) Close() error {
	leveldbConn.levelDB.Close()
	return nil
}

func (leveldbConn *LeveldbConn) GetKeyVal(key []byte) ([]byte, error) {
	s, err := leveldbConn.levelDB.Get(key, nil)
	if err != nil && strings.Contains(err.Error(), "not found") {
		return nil, nil
	}
	return s, err
}

func (leveldbConn *LeveldbConn) PutKeyVal(key []byte, val []byte) error {
	err := leveldbConn.levelDB.Put(key, val, nil)
	return err
}

func (leveldbConn *LeveldbConn) DelKeyVal(key []byte) error {
	return leveldbConn.levelDB.Delete(key, nil)
}

func (leveldbConn *LeveldbConn) GetListByKeyPrefix(key []byte) ([]driver.ListValue, error) {
	data := make([]driver.ListValue, 0)
	iter := leveldbConn.levelDB.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		data = append(data,
			driver.ListValue{
				Key:   string(iter.Key()),
				Value: string(iter.Value()),
			})
	}
	iter.Release()
	return data, nil
}
