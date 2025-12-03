package pebble

import "github.com/brokercap/Bifrost/xdb/driver"

const VERSION = "v1.1.0"

func init() {
	driver.Register("pebble", &PebbleDriver{}, VERSION)
}
