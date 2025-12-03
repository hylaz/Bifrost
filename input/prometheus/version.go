package prometheus

import inputDriver "github.com/brokercap/Bifrost/input/driver"

const (
	VERSION         string = "v2.0.4"
	BIFROST_VERSION string = "v2.0.3"
)

const InputName = "prometheus"

func init() {
	inputDriver.Register(InputName, NewInputPrometheus, VERSION, BIFROST_VERSION)
}
