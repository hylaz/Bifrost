package xgo

var sessionMgr *SessionMgr = nil //session管理器

func StartSession(cookieName ...string) {
	if len(cookieName) == 0 {
		sessionMgr = NewSessionMgr("xgo_cookie", 3600)
	} else {
		sessionMgr = NewSessionMgr(cookieName[0], 3600)
	}
}
