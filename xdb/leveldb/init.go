package leveldb

import "github.com/brokercap/Bifrost/xdb/driver"

const Version = "v1.1.0"

func init() {
	driver.Register("leveldb", &LeveldbDriver{}, Version)
}
