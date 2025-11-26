package admin

import (
	"github.com/brokercap/Bifrost/admin/controller"
	_ "github.com/brokercap/Bifrost/admin/router"
	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/brokercap/Bifrost/config"
	"log"
	"runtime/debug"
)

func Start() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	xgo.StartSession()
	xgo.AddStaticRoute("/css/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/js/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/fonts/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/img/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/plugin/", config.BifrostPluginTemplateDir)
	var err error
	if config.TLS {
		err = xgo.StartTLS(config.Listen, config.TLSServerKeyFile, config.TLSServerCrtFile)
	} else {
		err = xgo.Start(config.Listen)
	}
	if err != nil {
		log.Println("Manager Start Err:", err)
	}
}
