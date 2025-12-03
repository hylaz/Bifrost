package bbolt

import (
	"bytes"
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	bolt "go.etcd.io/bbolt"
	"os"
	"time"
)

const BoltBucket = "bolt_bucket"

type BboltDriver struct{}

func (bboltDriver *BboltDriver) Open(path string) (driver.XdbDriver, error) {
	return newConn(path)
}

func newConn(path string) (*BboltConn, error) {

	if path == "" {
		return nil, fmt.Errorf("path error")
	}

	f := &BboltConn{
		path: path,
	}
	err := f.connect()
	if err != nil {
		return nil, err
	}
	return f, nil
}

type BboltConn struct {
	path   string
	err    error
	bucket string
	db     *bolt.DB
}

func (bboltConn *BboltConn) connect() error {
	os.MkdirAll(bboltConn.path, 0755)

	opts := &bolt.Options{
		Timeout:      1 * time.Second,
		NoGrowSync:   false,
		FreelistType: bolt.FreelistMapType,
	}

	bboltConn.db, bboltConn.err = bolt.Open(bboltConn.path, 0755, opts)

	return bboltConn.err
}

func (bboltConn *BboltConn) Close() error {
	err := bboltConn.db.Close()
	return err
}

func (bboltConn *BboltConn) GetKeyVal(key []byte) ([]byte, error) {

	var value []byte
	err := bboltConn.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bboltConn.bucket)
		}

		v := b.Get(key)
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})

	return value, err

}

func (bboltConn *BboltConn) PutKeyVal(key []byte, val []byte) error {
	return bboltConn.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bboltConn.bucket))
		if err != nil {
			return err
		}
		return b.Put(key, val)
	})

}

func (bboltConn *BboltConn) DelKeyVal(key []byte) error {
	return bboltConn.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucket))
		if b == nil {
			return nil
		}
		return b.Delete(key)
	})

}

func (bboltConn *BboltConn) GetListByKeyPrefix(prefix []byte) ([]driver.ListValue, error) {

	var results []driver.ListValue

	err := bboltConn.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bboltConn.bucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()

		// 定位到前缀开始位置
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			kv := driver.ListValue{
				Key:   string(k),
				Value: string(v),
			}
			results = append(results, kv)
		}
		return nil
	})
	return results, err

}
