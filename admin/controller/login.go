package controller

import (
	"net/http"

	"github.com/brokercap/Bifrost/server/user"
)

type LoginController struct {
	UserController
}

func (c *LoginController) Index() {
	c.SetTitle("Login")
	c.AddAdminTemplate("login.html")
}

func (c *LoginController) Login() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.UserName == "" {
		result.Msg = "user not exist"
		return
	}
	var sessionID = c.Ctx.Session.StartSession(c.Ctx.ResponseWriter, c.Ctx.Request)
	mayXRealIP, remoteAddrIp := c.GetRemoteIp()
	UserInfo, err := user.CheckUserWithIP(param.UserName, param.Password, mayXRealIP, remoteAddrIp)
	if err == nil {
		c.Ctx.Session.SetSessionVal(sessionID, "UserName", param.UserName)
		c.Ctx.Session.SetSessionVal(sessionID, "Group", UserInfo.Group)
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
		return
	}
	result.Msg = err.Error()
	return
}

func (c *LoginController) Logout() {
	c.Ctx.Session.EndSession(c.Ctx.ResponseWriter, c.Ctx.Request)
	if c.IsHtmlOutput() {
		c.SetOutputByUser()
		http.Redirect(c.Ctx.ResponseWriter, c.Ctx.Request, "/login/index", http.StatusFound)
	} else {
		result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
	}
}
