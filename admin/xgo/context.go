package xgo

import (
	"net/http"
	"strconv"
	"strings"
)

type Context struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	Session        *SessionMgr
}

func (ctx *Context) GetParamInt64(key string, defaultVal ...int64) (int64, error) {
	val := ctx.Request.Form.Get(key)
	int64, err := strconv.ParseInt(val, 10, 64)
	if len(defaultVal) == 0 {
		return int64, err
	}
	if err != nil {
		return defaultVal[0], err
	}
	return int64, err
}

func (ctx *Context) GetParamUInt64(key string, defaultVal ...uint64) (uint64, error) {
	val := ctx.Request.Form.Get(key)
	uint64, err := strconv.ParseUint(val, 10, 64)
	if len(defaultVal) == 0 {
		return uint64, err
	}
	if err != nil {
		return defaultVal[0], err
	}
	return uint64, err
}

func (ctx *Context) GetParamBool(key string, defaultVal ...bool) bool {
	val := ctx.Request.Form.Get(key)
	if val == "" || len(defaultVal) > 0 {
		return defaultVal[0]
	}
	if strings.ToLower(val) == "true" {
		return true
	} else {
		return false
	}
}

func (ctx *Context) Get(key string, defaultVal ...string) string {
	val := ctx.Request.Form.Get(key)
	if val == "" && len(defaultVal) > 0 {
		return defaultVal[0]
	}
	return ctx.Request.Form.Get(key)
}
