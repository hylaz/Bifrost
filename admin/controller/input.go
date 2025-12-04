package controller

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

type InputController struct {
	CommonController
}

func (c *InputController) List() {
	driversMap := inputDriver.Drivers()
	c.SetJsonData(driversMap)
	c.StopServeJSON()
}
