package src

import (
	"strings"
)

func ParseDSN(dsn string) (params map[string]string) {
	params = make(map[string]string, 0)
	if dsn == "" {
		return
	}
	var index int
	var addr string
	var paramStr string
	index = strings.Index(dsn, "?")
	if index <= 0 {
		addr = dsn
	} else {
		addr = dsn[0:index]
		paramStr = dsn[index+1:]
	}
	params["addr"] = addr
	if paramStr == "" {
		return
	}
	for _, v := range strings.Split(paramStr, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		params[param[0]] = param[1]
	}
	return params
}
