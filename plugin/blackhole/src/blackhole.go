package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const Version = "v1.6.0"
const BifrostVersion = "v1.6.0"

func init() {
	pluginDriver.Register("blackhole", NewBlackholeConn, Version, BifrostVersion)
}

func NewBlackholeConn() pluginDriver.Driver {
	return &BlackholeConn{}
}

type BlackholeConn struct {
	pluginDriver.PluginDriverInterface
}
