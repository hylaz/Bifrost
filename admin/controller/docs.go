package controller

import "github.com/brokercap/Bifrost/plugin/driver"

type DocsController struct {
	CommonController
}

func (c *DocsController) Index() {
	PluginKey := c.Ctx.Request.Form.Get("plugin")
	c.SetData("PluginKey", PluginKey)
	c.SetData("Drivers", driver.Drivers())
	c.SetData("Title", "docs")
	c.AddAdminTemplate("docs.html", "header.html", "footer.html")
}

func (c *DocsController) ApiDocIndex() {
	c.SetData("Title", "api_docs")
	c.AddAdminTemplate("api.doc.html", "header.html", "footer.html")
}
