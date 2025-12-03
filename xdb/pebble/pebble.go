package pebble

import (
	"errors"
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/cockroachdb/pebble"
	"os"
)

type PebbleDriver struct{}

func (pebbleDriver *PebbleDriver) Open(path string) (driver.XdbDriver, error) {
	return newConn(path)
}

func newConn(path string) (*PebbleConn, error) {

	if path == "" {
		return nil, fmt.Errorf("path error")
	}
	f := &PebbleConn{
		path: path,
	}

	err := f.connect()
	if err != nil {
		return nil, err
	}
	return f, nil
}

type PebbleConn struct {
	path string
	err  error
	db   *pebble.DB
}

func (pebbleConn *PebbleConn) connect() error {
	os.MkdirAll(pebbleConn.path, 0755)

	pebbleConn.db, pebbleConn.err = pebble.Open(pebbleConn.path, &pebble.Options{})

	return pebbleConn.err
}

func (pebbleConn *PebbleConn) Close() error {
	err := pebbleConn.db.Close()
	return err
}

func (pebbleConn *PebbleConn) GetKeyVal(key []byte) ([]byte, error) {

	// 检查是否存在
	val, closer, err := pebbleConn.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	defer closer.Close()
	return val, nil

}

func (pebbleConn *PebbleConn) PutKeyVal(key []byte, val []byte) error {
	err := pebbleConn.db.Set(key, val, pebble.Sync)
	return err
}

func (pebbleConn *PebbleConn) DelKeyVal(key []byte) error {
	// 删除单个key
	err := pebbleConn.db.Delete(key, pebble.Sync)
	return err

}

// 计算前缀的上界
func prefixUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // 没有上界
}

func (pebbleConn *PebbleConn) GetListByKeyPrefix(prefix []byte) ([]driver.ListValue, error) {

	var results []driver.ListValue

	// 创建迭代器选项
	iterOptions := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	}

	iter, err := pebbleConn.db.NewIter(iterOptions)
	if err != nil {
		return results, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val, err := iter.ValueAndErr()
		if err != nil {
			return results, err
		}

		kv := driver.ListValue{
			Key:   string(key),
			Value: string(val),
		}
		results = append(results, kv)
	}
	return results, iter.Error()

}
