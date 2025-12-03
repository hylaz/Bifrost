package prometheus

import inputDriver "github.com/brokercap/Bifrost/input/driver"

func (c *InputPrometheus) IsSupported(supportType inputDriver.SupportType) bool {
	switch supportType {
	case inputDriver.SupportIncre:
		return true

		// 需要由上一层server层定时计算最小的位点提交进来
	case inputDriver.SupportNeedMinPosition:
		return false
	}
	return false
}
