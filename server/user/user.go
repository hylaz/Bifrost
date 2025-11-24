package user

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/server/storage"
)

const USER_PREFIX string = "bifrost:userList:"

type UserGroupType string

type UserInfo struct {
	Name       string
	Password   string
	Group      string
	Host       string
	AddTime    int64
	UpdateTime int64
}

func init() {

}

func getUserGroup(groupName string) string {
	if groupName != "administrator" {
		return "monitor"
	}
	return groupName
}

func InitUser() {
	userList := storage.GetListByPrefix([]byte(USER_PREFIX))
	if len(userList) != 0 {
		return
	}
	func() {
		for Name, Password := range config.GetConf("user") {
			UserGroup := getUserGroup(config.GetConfigVal("groups", Name))
			User := UserInfo{
				Name:       Name,
				Password:   Password,
				Group:      UserGroup,
				Host:       "%",
				AddTime:    time.Now().Unix(),
				UpdateTime: time.Now().Unix(),
			}
			b, _ := json.Marshal(User)
			err := storage.PutKeyVal([]byte(USER_PREFIX+Name), b)
			if err != nil {
				log.Println("InitUser error:", err, " user:", User)
			}
		}
	}()
}

func RecoveryUser(content *json.RawMessage) {
	if content == nil {
		return
	}
	var data []*UserInfo
	errors := json.Unmarshal(*content, &data)
	if errors != nil {
		log.Println("recovery user content errors;", errors, " content:", content)
		return
	}

	for _, User := range data {
		b, _ := json.Marshal(User)
		storage.PutKeyVal([]byte(USER_PREFIX+User.Name), b)
	}
}

func GetUserList() []UserInfo {
	userListString := storage.GetListByPrefix([]byte(USER_PREFIX))
	if len(userListString) == 0 {
		return []UserInfo{}
	}
	UserList := make([]UserInfo, 0)
	for _, v := range userListString {
		var User UserInfo
		err := json.Unmarshal([]byte(v.Value), &User)
		if err == nil {
			UserList = append(UserList, User)
			if User.Name == "" {
				storage.DelKeyVal([]byte(v.Key))
			}
		}
	}
	return UserList
}

func DelUser(Name string) error {
	key := USER_PREFIX + Name
	return storage.DelKeyVal([]byte(key))
}

func AddUser(Name, Password, GroupName string, Host string) error {
	return UpdateUser(Name, Password, GroupName, Host)
}

func UpdateUser(Name, Password, GroupName string, Host string) error {
	if Name == "" || Password == "" {
		return fmt.Errorf("name and password not be empty")
	}
	OldUserInfo := GetUserInfo(Name)
	User := &UserInfo{
		Name:     Name,
		Password: Password,
		Host:     Host,
		Group:    getUserGroup(GroupName),
	}
	if OldUserInfo.Name == "" {
		User.AddTime = time.Now().Unix()
		User.UpdateTime = time.Now().Unix()
	} else {
		User.AddTime = OldUserInfo.AddTime
		User.UpdateTime = time.Now().Unix()
	}
	key := USER_PREFIX + Name
	b, _ := json.Marshal(User)
	return storage.PutKeyVal([]byte(key), b)
}

func GetUserInfo(Name string) *UserInfo {
	b, err := storage.GetKeyVal([]byte(USER_PREFIX + Name))
	if err != nil {
		return &UserInfo{}
	}
	var User UserInfo
	err = json.Unmarshal(b, &User)
	if err != nil {
		return &UserInfo{}
	}
	return &User
}

func CheckUser(Name, Password string) (userInfo *UserInfo, err error) {
	userInfo = GetUserInfo(Name)
	if userInfo.Name == "" {
		err = errors.New("user not exist")
		return
	}
	if userInfo.Password != Password {
		err = errors.New("password error")
		return
	}
	return
}

// IP 有可能是nginx代理转发采用的 X-Real-IP
// RemoteAddrIp 直接采用的是HTTP源头的IP
// RemoteAddrIp 假如是 127.0.0.1(进程当前机器访问) 可直接跳过，不进行验证
func CheckUserWithIP(Name, Password string, IP string, RemoteAddrIp string) (userInfo *UserInfo, err error) {
	if RemoteAddrIp != "127.0.0.1" && CheckRefuseIp(IP) {
		return nil, errors.New("ip is refused")
	}
	userInfo, err = CheckUser(Name, Password)
	if err != nil {
		AddFailedIp(IP)
		appendLoginLog("IP:%s UserName:%s Password:%s login failed", IP, Name, Password)
		return nil, errors.New("user or password error")
	}
	err = CheckUserHost(IP, userInfo.Host)
	if userInfo.Group == "" {
		userInfo.Group = "monitor"
	}
	if err != nil {
		AddFailedIp(IP)
		appendLoginLog("IP:%s UserName:%s CheckUserHost failed", IP, Name)
	} else {
		appendLoginLog("IP:%s UserName:%s login success", IP, Name)
	}
	return
}

func CheckUserHost(IP, Host string) (err error) {
	if IP == Host {
		return
	}
	switch Host {
	case "", "%":
		return
	default:
		break
	}
	ipArr := strings.Split(IP, ".")
	var ok bool
	for _, HostName := range strings.Split(Host, ",") {
		ok = CheckUserHost0(ipArr, HostName)
		if ok {
			return nil
		}
	}
	return errors.New("no login permission")
}

func CheckUserHost0(ipArr []string, Host string) bool {
	switch Host {
	case "%":
		return true
	default:
		break
	}
	hostArr := strings.Split(Host, ".")
	for i, v := range hostArr {
		if v == "%" {
			continue
		}
		if ipArr[i] == v {
			continue
		}
		return false
	}
	return true
}
