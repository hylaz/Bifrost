package kafka

import inputDriver "github.com/brokercap/Bifrost/input/driver"

func (c *InputKafka) IsSupported(supportType inputDriver.SupportType) bool {
	switch supportType {
	case inputDriver.SupportIncre:

		return true
	case inputDriver.SupportNeedMinPosition:

		return true
	}
	return false
}
