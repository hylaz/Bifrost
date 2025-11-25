package warning

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/server/storage"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const WARNING_KEY_PREFIX = "bifrost:warning:config:"

type WaringConfig struct {
	Type  string
	Param map[string]interface{}
}

var allWaringConfigCacheMap map[string]WaringConfig

var firstStartUp = true
var lastConfigID = 0

func init() {
	allWaringConfigCacheMap = make(map[string]WaringConfig, 0)
}

func getLastConfigId() int {
	l.Lock()
	defer l.Unlock()
	lastConfigID++
	return lastConfigID
}

func getNewWaringKey() string {
	return WARNING_KEY_PREFIX + strconv.Itoa(getLastConfigId())
}

func getWaringKey(id int) string {
	return WARNING_KEY_PREFIX + strconv.Itoa(id)
}

func InitWarningConfigCache() {
	if firstStartUp == false {
		return
	}
	firstStartUp = false

	data := storage.GetListByPrefix([]byte(WARNING_KEY_PREFIX))
	for _, v := range data {
		key := v.Key
		t := strings.Split(key, ":")
		id, err := strconv.Atoi(t[len(t)-1])
		if err != nil {
			continue
		}
		if id > lastConfigID {
			lastConfigID = id
		}
		var tmpWarningConfig WaringConfig
		err2 := json.Unmarshal([]byte(v.Value), &tmpWarningConfig)
		if err2 != nil {
			continue
		}
		addWarningConfigCache(key, tmpWarningConfig)
	}
}

func GetWarningConfigList() map[string]WaringConfig {
	InitWarningConfigCache()
	return allWaringConfigCacheMap
}

func addWarningConfigCache(key string, config WaringConfig) {
	l.Lock()
	allWaringConfigCacheMap[key] = config
	l.Unlock()
}

func delWarningConfigCache(key string) {
	l.Lock()
	delete(allWaringConfigCacheMap, key)
	l.Unlock()
}

func AddNewWarningConfig(p WaringConfig) (string, error) {
	InitWarningConfigCache()
	b, _ := json.Marshal(p)
	key := getNewWaringKey()
	addWarningConfigCache(key, p)
	return key, storage.PutKeyVal([]byte(key), b)
}

func DelWarningConfig(ID int) error {
	key := getWaringKey(ID)
	delWarningConfigCache(key)
	return storage.DelKeyVal([]byte(key))
}

func RecoveryWarning(content *json.RawMessage) {
	if content == nil {
		return
	}
	var data map[string]WaringConfig
	errors := json.Unmarshal(*content, &data)
	if errors != nil {
		logrus.Println("recorery warning content errors;", errors, " content:", content)
		return
	}
	var i int
	var Id string
	for key, v := range data {
		i = strings.LastIndexAny(key, ":")
		if i < 1 {
			continue
		}
		Id = key[i+1:]
		b, _ := json.Marshal(v)
		storage.PutKeyVal([]byte(WARNING_KEY_PREFIX+Id), b)
	}
	firstStartUp = true
}
