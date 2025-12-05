package admin

import (
	_ "github.com/brokercap/Bifrost/admin/router"
	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/brokercap/Bifrost/config"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

func Start() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	xgo.StartSession()
	xgo.AddStaticRoute("/css/", "public")
	xgo.AddStaticRoute("/js/", "public")
	xgo.AddStaticRoute("/fonts/", "public")
	xgo.AddStaticRoute("/img/", "public")
	//xgo.AddStaticRoute("/plugin/", config.BifrostPluginTemplateDir)
	var err error
	if config.Tls {
		err = xgo.StartTLS(config.Listen, config.TLSServerKeyFile, config.TLSServerCrtFile)
	} else {
		err = xgo.Start(config.Listen)
	}
	if err != nil {
		logrus.Println("Manager Start Err:", err)
	}
}
