package manager

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/sirupsen/logrus"
)

type TestController struct {
	xgo.Controller
}

func (c *TestController) Prepare() {
	logrus.Println("Prepare ..")
}

func (c *TestController) Finish() {
	logrus.Println("Finish ..")
}

func (c *TestController) Post() {
	logrus.Println("Post ..")
	c.Data["data"] = "success"
}
