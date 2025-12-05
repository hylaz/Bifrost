package xgo

var sessionMgr *SessionMgr

func StartSession(cookieName ...string) {
	if len(cookieName) == 0 {
		sessionMgr = NewSessionMgr("xgo_cookie", 3600)
	} else {
		sessionMgr = NewSessionMgr(cookieName[0], 3600)
	}
}
