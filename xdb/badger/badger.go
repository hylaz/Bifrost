package badger

import (
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/dgraph-io/badger/v4"
	"os"
)

type BadgerDriver struct{}

func (badgerDriver *BadgerDriver) Open(path string) (driver.XdbDriver, error) {
	return newConn(path)
}

func newConn(path string) (*BadgerConn, error) {

	if path == "" {
		return nil, fmt.Errorf("path error")
	}

	f := &BadgerConn{
		path: path,
	}

	err := f.connect()
	if err != nil {
		return nil, err
	}
	return f, nil
}

type BadgerConn struct {
	path string
	err  error
	db   *badger.DB
}

func (badgerConn *BadgerConn) connect() error {
	os.MkdirAll(badgerConn.path, 0755)
	badgerConn.db, badgerConn.err = badger.Open(badger.DefaultOptions(badgerConn.path))
	return badgerConn.err
}

func (badgerConn *BadgerConn) Close() error {
	err := badgerConn.db.Close()
	return err
}

func (badgerConn *BadgerConn) GetKeyVal(key []byte) ([]byte, error) {
	var value []byte
	err := badgerConn.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		// 获取值
		err = item.Value(func(val []byte) error {
			value = append(value, val...)
			return nil
		})
		return err
	})

	return value, err

}

func (badgerConn *BadgerConn) PutKeyVal(key []byte, val []byte) error {
	err := badgerConn.db.Update(func(txn *badger.Txn) error {
		// 设置键值对
		err := txn.Set(key, val)
		return err
	})
	return err
}

func (badgerConn *BadgerConn) DelKeyVal(key []byte) error {
	err := badgerConn.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
	return err

}

func (badgerConn *BadgerConn) GetListByKeyPrefix(prefix []byte) ([]driver.ListValue, error) {

	var results []driver.ListValue

	err := badgerConn.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			kv := driver.ListValue{
				Key:   string(item.Key()),
				Value: string(val),
			}
			results = append(results, kv)
		}
		return nil
	})
	return results, err
}
