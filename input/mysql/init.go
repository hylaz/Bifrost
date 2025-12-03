package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

const (
	Vesrion        string = "v2.3.12"
	BifrostVesrion string = "v2.3.12"
)

func init() {
	inputDriver.Register("mysql", NewInputPlugin, Vesrion, BifrostVesrion)
}
