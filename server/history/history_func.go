package history

import (
	"fmt"
	"github.com/brokercap/Bifrost/server"
	"strings"
)

func AddHistory(dbName string, SchemaName string, TableName string, TableNames string, Property HistoryProperty, ToServerIDList []int) (int, error) {
	l.Lock()
	defer l.Unlock()
	db := server.GetDBObj(dbName)
	if db == nil {
		return 0, fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName]; !ok {
		historyMap[dbName] = make(map[int]*History)
	}

	if Property.SyncThreadNum <= 0 {
		Property.SyncThreadNum = 1
	}

	Property.FirstLimitOptimize = Property.LimitOptimize
	if len(ToServerIDList)*Property.SyncThreadNum > 16384 {
		return 0, fmt.Errorf("SyncThreadNum * len(ToServerIDList) > 16384")
	}

	ID := lastHistoryID + 1
	TableNameArrTmp := strings.Split(TableNames, ";")
	TableNameArr := make([]*TableStatus, 0)
	for _, v := range TableNameArrTmp {
		if v == "" {
			continue
		}
		TableNameArr = append(TableNameArr, &TableStatus{RowsCount: 0, SelectCount: 0, TableName: strings.Trim(v, "")})
	}
	historyJob := &History{
		ID:                ID,
		DbName:            dbName,
		SchemaName:        SchemaName,
		TableName:         TableName,
		TableNames:        TableNames,
		TableNameArr:      TableNameArr,
		TableCount:        len(TableNameArr),
		TableCountSuccess: 0,
		CurrentTableName:  "",
		Status:            HISTORY_STATUS_CLOSE,
		NowStartI:         0,
		Property:          Property,
		ToServerIDList:    ToServerIDList,
		ThreadPool:        make([]*ThreadStatus, 0),
		Uri:               db.ConnectUri,
	}
	lastHistoryID = ID
	if Property.Crontab != "" {
		err := startCrond(historyJob)
		if err != nil {
			return 0, err
		}
	}
	historyMap[dbName][ID] = historyJob
	return ID, nil
}

func DelHistory(dbName string, id int) bool {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return true
	}
	_ = deleteCrond(historyMap[dbName][id])
	delete(historyMap[dbName], id)
	if len(historyMap[dbName]) == 0 {
		delete(historyMap, dbName)
	}
	return true
}

// KillHistory 杀死同步
func KillHistory(dbName string, ID int) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}

	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}

	historyMap[dbName][ID].Status = HISTORY_STATUS_KILLED
	for _, toServer := range historyMap[dbName][ID].ToServerList {
		toServer.ToServerInfo.Status = "deled"
	}
	return nil
}

// StopHistory 暂停同步
func StopHistory(dbName string, ID int) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}
	historyMap[dbName][ID].Status = HISTORY_STATUS_SELECT_STOPING
	return nil
}

func GetHistoryList(dbName, SchemaName, TableName string, status HisotryStatus) []History {
	l.RLock()
	defer l.RUnlock()
	data := make([]History, 0)
	for dbNameKey, v := range historyMap {
		if dbName != "" {
			if dbNameKey != dbName {
				continue
			}
		}
		for _, historyInfo := range v {
			if SchemaName != "" {
				if SchemaName != historyInfo.SchemaName {
					continue
				}
				if TableName != "" {
					if TableName != historyInfo.TableName {
						continue
					}
				}
			}
			if status != HISTORY_STATUS_ALL {
				if historyInfo.Status != status {
					continue
				}
			}
			if historyInfo.cronEntryID > 0 {
				historyInfo.ContabNextTime = crodObj.Entry(historyInfo.cronEntryID).Next
			}
			data = append(data, *historyInfo)
		}
	}
	return data
}
