package mongo

import inputDriver "github.com/brokercap/Bifrost/input/driver"

func (c *MongoInput) IsSupported(supportType inputDriver.SupportType) bool {
	switch supportType {
	case inputDriver.SupportIncre:
		return true
	case inputDriver.SupportNeedMinPosition:
		return false
	}
	return false
}
