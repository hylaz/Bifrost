package xgo

import (
	"encoding/json"
	"errors"
	"github.com/brokercap/Bifrost/admin/view"
	"github.com/sirupsen/logrus"
	"html/template"
	"strings"
)

var (
	AbortErr = errors.New("user stop run")
)

func init() {

}

type OutputFormat int8

const (
	JsonType  OutputFormat = 1
	JsonpType OutputFormat = 2
	HtmlTYPE  OutputFormat = 0
	OtherType OutputFormat = -1
)

type Controller struct {
	Ctx            *Context
	ControllerName string
	ActionName     string
	Data           map[string]interface{}
	Format         OutputFormat
	Template       *template.Template
	tplArr         []string // 模板路径
}

// ControllerInterface is an interface to uniform all controller handler.
type ControllerInterface interface {
	Init(ct *Context, controllerName, actionName string)
	Prepare()
	Finish()
	StopServeJSON()
	StopServeJSONP(jsonp ...string)
	StopRun()
	NormalStop()
	IsHtmlOutput() bool
	SetOutputByUser()
	AddTemplate(tpl ...string)
	SetTemplate(t *template.Template, err error)
}

func (c *Controller) Init(ctx *Context, controllerName, actionName string) {
	ctx.Request.ParseForm()
	c.Ctx = ctx
	c.Data = make(map[string]interface{}, 0)
	c.ControllerName = controllerName
	c.ActionName = actionName
}

func (c *Controller) Prepare() {

}

func (c *Controller) Finish() {

}

func (c *Controller) SetOutputByUser() {
	c.Format = OtherType
}

func (c *Controller) AddTemplate(tpl ...string) {
	c.tplArr = append(c.tplArr, tpl...)
}

func (c *Controller) SetTemplate(t *template.Template, err error) {
	if err != nil {
		panic(err.Error())
	}
	c.Template = t
}

func (c *Controller) SetData(key string, data interface{}) {
	c.Data[key] = data
}

func (c *Controller) SetJsonData(data interface{}) {
	c.Data["json"] = data
}

func (c *Controller) StopServeJSON() {
	c.Format = JsonType
	c.StopRun()
	panic(AbortErr)
}

func (c *Controller) StopServeJSONP(jsonp ...string) {
	c.Format = JsonpType
	c.StopRun()
	panic(AbortErr)
}

func (c *Controller) StopRun() {
	c.NormalStop()
	panic(AbortErr)
}

func (c *Controller) NormalStop() {
	c.Finish()
	if c.Format == HtmlTYPE {
		switch strings.ToLower(c.Ctx.Request.Form.Get("format")) {
		case "json":
			c.Format = JsonType
			break
		case "jsonp":
			c.Format = JsonpType
			break
		default:
			if c.Ctx.Request.Header.Get("X-Requested-With") == "XMLHttpRequest" {
				c.Format = JsonType
				break
			}
			if c.Template == nil && len(c.tplArr) == 0 {
				c.Format = JsonType
			}
			break
		}
	}
	c.Ctx.ResponseWriter.WriteHeader(200)
	switch c.Format {
	case JsonType:
		var body []byte
		if _, ok := c.Data["json"]; ok {
			body, _ = json.Marshal(c.Data["json"])
		} else {
			body, _ = json.Marshal(c.Data)
		}
		c.Ctx.ResponseWriter.Write(body)
		break
	case JsonpType:
		var body []byte
		if _, ok := c.Data["json"]; ok {
			body, _ = json.Marshal(c.Data["json"])
		} else {
			body, _ = json.Marshal(c.Data)
		}
		c.Ctx.ResponseWriter.Write([]byte(body))
		break
	case OtherType:
		break
	default:
		if c.Template == nil {
			var err error
			//c.Template, err = template.ParseFiles(c.tplArr...)
			c.Template, err = template.ParseFS(view.EmbedTemplate, c.tplArr...)
			if err != nil {
				logrus.Println("err:", err)
				panic(err.Error())
			}
		}
		err := c.Template.Execute(c.Ctx.ResponseWriter, c.Data)
		if err != nil {
			panic(err)
		}
		break
	}
}

func (c *Controller) IsHtmlOutput() bool {
	if c.Format == HtmlTYPE {
		switch strings.ToLower(c.Ctx.Request.Form.Get("format")) {
		case "json":
			c.Format = JsonType
			break
		case "jsonp":
			c.Format = JsonpType
			break
		default:
			if c.Ctx.Request.Header.Get("X-Requested-With") == "XMLHttpRequest" {
				c.Format = JsonType
				break
			}
			break
		}
	}
	if c.Format == HtmlTYPE {
		return true
	}

	return false
}
