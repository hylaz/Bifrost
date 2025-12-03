package mongo

import inputDriver "github.com/brokercap/Bifrost/input/driver"

func init() {
	inputDriver.Register("mongo", NewInputPlugin, Vesrion, BifrostVesrion)
}
