package redis

import "github.com/brokercap/Bifrost/xdb/driver"

const Version = "v1.1.1"

func init() {
	driver.Register("redis", &RedisDriver{}, Version)
}
