package src

import (
	dbDriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/sirupsen/logrus"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType
}

func init() {
	pluginDriver.Register(OutputName, NewMysqlConn, Version, BifrostVersion)
}

type fieldStruct struct {
	ToField        string
	FromMysqlField string
	ToFieldType    string
	ToFieldDefault *string
}

func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType, 0)
	CommitData = append(CommitData, nil)
	return &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: CommitData,
	}
}

type MysqlConn struct {
	uri              string
	status           string
	p                *PluginParam
	db               *mysqlDB
	err              error
	serverVersion    string
	isTiDB           bool
	isStarRocks      bool
	starRocksBeCount int
}

type PluginParam struct {
	Field                []fieldStruct
	BatchSize            int
	Schema               string
	Table                string
	NullTransferDefault  bool //是否将null值强制转成相对应类型的默认值
	SyncMode             SyncMode
	BifrostMustBeSuccess bool // bifrost server 保留,数据是否能丢

	schemaAndTable string
	replaceInto    bool // 记录当前表是否有replace into操作
	PriKey         []fieldStruct
	toPriKey       string // toMysql 主键字段
	fromPriKey     string //	对应 from mysql 的主键id
	Data           *TableDataStruct
	fieldCount     int
	tableMap       map[string]*PluginParam0 // 需要自动创建ck表结构 创建之后表基本信息
	toDatabaseMap  map[string]bool          // ck 里,database 列表信息，database name 做为key，用于缓存
	AutoTable      bool                     // 是否自动匹配数据表
	stmtArr        []dbDriver.Stmt
	SkipBinlogData *pluginDriver.PluginDataType // 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}

type PluginParam0 struct {
	Field          []fieldStruct
	SchemaName     string
	TableName      string
	SchemaAndTable string
	PriKey         []fieldStruct // 主键对应关系
	FromPriKey     string        // 源表的 主键 字段
	ToPriKey       string        // 目标库的 主键 字段
}

func NewMysqlConn() pluginDriver.Driver {
	return &MysqlConn{status: "close"}
}

func (conn *MysqlConn) SetOption(uri *string, param map[string]interface{}) {
	conn.uri = *uri
	return
}

func (conn *MysqlConn) Open() error {
	conn.Connect()
	return nil
}

func (conn *MysqlConn) CheckUri() error {
	conn.Connect()
	if conn.db.err != nil {
		return conn.db.err
	}
	if conn.db == nil {
		conn.Close()
		return fmt.Errorf("connect error")
	}

	var schemaList []string
	func() {
		defer func() {
			return
		}()
		schemaList = conn.db.GetSchemaList()
	}()

	if len(schemaList) == 0 {
		conn.Close()
		return fmt.Errorf("schema count is 0 (not in system)")
	}
	return nil
}

func (conn *MysqlConn) GetUriExample() string {
	return "root:root@tcp(127.0.0.1:3306)/test"
}

func (conn *MysqlConn) initTableInfo() {
	if conn.p.AutoTable == false {
		conn.initToMysqlTableFieldType()
	} else {
		conn.p.tableMap = make(map[string]*PluginParam0, 0)
		conn.initToDatabaseMap()
	}
}

func (conn *MysqlConn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.BatchSize == 0 {
		param.BatchSize = 500
	}
	if param.Table == "" {
		param.AutoTable = true
	}
	param.Data = NewTableData()
	if param.AutoTable == false {
		param.schemaAndTable = "`" + param.Schema + "`.`" + param.Table + "`"
		param.toPriKey = param.PriKey[0].ToField
		param.fromPriKey = param.PriKey[0].FromMysqlField
	}
	param.stmtArr = make([]dbDriver.Stmt, 4)
	if param.SyncMode == "" {
		param.SyncMode = SYNCMODE_NORMAL
	}

	conn.p = &param
	conn.initTableInfo()
	conn.initVersion()
	if !conn.isTiDB {
		// 假如是TiDB,则说明肯定不是starrocks
		conn.initIsStarrock()
	}
	return conn.p, nil
}

func (conn *MysqlConn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		conn.p = p.(*PluginParam)
		return p, nil
	default:
		return conn.GetParam(p)
	}
}

func (conn *MysqlConn) initToMysqlTableFieldType() {
	defer func() {
		if err := recover(); err != nil {
			logrus.Println(string(debug.Stack()))
			conn.db.err = fmt.Errorf(string(debug.Stack()))
		}
	}()
	if conn.p == nil {
		return
	}

	fields := conn.db.GetTableFields(conn.p.Schema, conn.p.Table)
	if conn.db.err != nil {
		conn.err = conn.db.err
		return
	}
	if len(fields) == 0 {
		return
	}
	ckFieldsMap := make(map[string]TableStruct)
	for _, v := range fields {
		ckFieldsMap[v.COLUMN_NAME] = v
	}
	list := make([]fieldStruct, 0)
	for k, v := range conn.p.Field {
		conn.p.Field[k].ToFieldType = ckFieldsMap[v.ToField].DATA_TYPE
		if strings.ToLower(ckFieldsMap[v.ToField].EXTRA) == "auto_increment" {
			conn.p.Field[k].ToFieldDefault = nil
			if v.FromMysqlField != "" {
				list = append(list, conn.p.Field[k])
			}
		} else {
			// mysql 里的默认值是在 insert 语句执行的时候，sql 里没有指定字段名的情况下，自动填充
			// 假如有默认值 ，但是允许为 null 的时候，假如 sql 里指定值为 null，还是可以将 null 写进去的
			// 但是 bf 同步写数据的时候，源端是可能为 null ，目标表 是 not null default 值
			// 因为后面 tansfer 函数只使用了 default 值，没做是否可以为 null 判断 ，这里进行统一判断 可以为 null 的情况下，默认值为 null
			if strings.ToUpper(ckFieldsMap[v.ToField].IS_NULLABLE) == "YES" {
				conn.p.Field[k].ToFieldDefault = nil
			} else {
				conn.p.Field[k].ToFieldDefault = ckFieldsMap[v.ToField].COLUMN_DEFAULT
			}
			list = append(list, conn.p.Field[k])
		}
	}
	conn.p.Field = list
	conn.p.fieldCount = len(list)
}

func (conn *MysqlConn) GetSchemaName(data *pluginDriver.PluginDataType) (SchemaName string) {
	if conn.p.Schema == "" {
		SchemaName = data.SchemaName
	} else {
		SchemaName = conn.p.Schema
	}
	return
}

func (conn *MysqlConn) GetTableName(data *pluginDriver.PluginDataType) (TableName string) {
	if conn.p.Table == "" {
		TableName = data.TableName
	} else {
		TableName = conn.p.Table
	}
	return
}

func (conn *MysqlConn) GetSchemaAndTable(data *pluginDriver.PluginDataType) (SchemaName, TableName, SchemaAndTable string) {
	SchemaName = conn.GetSchemaName(data)
	TableName = conn.GetTableName(data)
	SchemaAndTable = fmt.Sprintf("`%s`.`%s`", SchemaName, TableName)
	return
}

func (conn *MysqlConn) CreateTableAndGetTableFieldsType(data *pluginDriver.PluginDataType) (tableFields *PluginParam0, err error) {
	tableFields, _ = conn.getAutoTableFieldType(data)
	// 这里无视 是否返回 error, 因为有可能会返回 查不到表的 错误,这里直接跳过这个错误,后面遇到错误会进进行再处理
	/*
		if err != nil {
			return nil, err
		}
	*/
	if tableFields != nil {
		return tableFields, nil
	}
	// 这里无视是否创建成功,如果失败了,后面的建表逻辑,也肯定报错

	_ = conn.db.CreateDatabase(conn.GetSchemaName(data))
	createTableSql, isContinue := conn.TransferToCreateTableSql(data)
	if createTableSql == "" {
		if isContinue {
			return nil, nil
		} else {
			logrus.Printf("[ERROR] output[%s] get create table sql is empty,data:%+v \n", OutputName, data)
			return nil, errors.New("get create table sql is empty")
		}
	}

	err = conn.db.Exec(createTableSql)
	if err != nil {
		return nil, err
	}
	tableFields, err = conn.getAutoTableFieldType(data)
	return
}

func (conn *MysqlConn) getAutoTableFieldType(data *pluginDriver.PluginDataType) (*PluginParam0, error) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Println(string(debug.Stack()))
			conn.db.err = fmt.Errorf(string(debug.Stack()))
		}
	}()
	var SchemaName, TableName, key = conn.GetSchemaAndTable(data)
	if _, ok := conn.p.tableMap[key]; ok {
		return conn.p.tableMap[key], nil
	}
	fields := conn.db.GetTableFields(SchemaName, TableName)
	if conn.db.err != nil {
		conn.err = conn.db.err
		return nil, conn.err
	}
	if len(fields) == 0 {
		err := fmt.Errorf("SchemaName:%s, TableName:%s not exsit", SchemaName, data.TableName)
		return nil, err
	}
	fieldList := make([]fieldStruct, 0)
	priKeyList := make([]fieldStruct, 0)
	var fromPriKey, toPriKey string
	var ok bool
	for _, v := range fields {
		var fromFieldName string
		if _, ok = data.Rows[0][v.COLUMN_NAME]; !ok {
			switch v.COLUMN_NAME {
			case "binlog_event_type":
				fromFieldName = "{$EventType}"
				break
			case "binlog_timestamp", "binlogtimestamp":
				fromFieldName = "{$BinlogTimestamp}"
				break
			case "binlogfilenum", "binlog_filenum":
				fromFieldName = "{$BinlogFileNum}"
				break
			case "binlogposition", "binlog_position":
				fromFieldName = "{$BinlogPosition}"
				break
			case "binlog_datetime", "binlogdatetime":
				fromFieldName = "{$BinlogDateTime}"
				break
			default:
				fromFieldName = v.COLUMN_NAME
				break
			}
		} else {
			fromFieldName = v.COLUMN_NAME
		}
		var ToFieldDefault *string
		// mysql 里的默认值是在 insert 语句执行的时候，sql 里没有指定字段名的情况下，自动填充
		// 假如有默认值 ，但是允许为 null 的时候，假如 sql 里指定值为 null，还是可以将 null 写进去的
		// 但是 bf 同步写数据的时候，源端是可能为 null ，目标表 是 not null default 值
		// 因为后面 tansfer 函数只使用了 default 值，没做是否可以为 null 判断 ，这里进行统一判断 可以为 null 的情况下，默认值为 null
		// 同 initToMysqlTableFieldType 函数内部注释
		if strings.ToUpper(v.IS_NULLABLE) == "YES" {
			ToFieldDefault = nil
		} else {
			ToFieldDefault = v.COLUMN_DEFAULT
		}
		field := fieldStruct{ToField: v.COLUMN_NAME, ToFieldType: v.DATA_TYPE, FromMysqlField: fromFieldName, ToFieldDefault: ToFieldDefault}
		if strings.ToUpper(v.COLUMN_KEY) == "PRI" {
			field.ToFieldDefault = nil
			priKeyList = append(priKeyList, field)
			if fromPriKey == "" || v.EXTRA == "auto_increment" {
				fromPriKey = v.COLUMN_NAME
				toPriKey = v.COLUMN_NAME
			}
		}
		// 假如starrocks表字段允许为null
		// 并且同时是BIGINT类型
		// 同时源端数据中又不存在,则认为其为自增字段,
		if v.COLUMN_DEFAULT == nil && strings.Contains(strings.ToUpper(v.DATA_TYPE), "BIGINT") {
			if _, ok = data.Rows[len(data.Rows)-1][v.COLUMN_NAME]; !ok {
				continue
			}
		}
		fieldList = append(fieldList, field)
	}
	if len(fieldList) == 0 {
		return nil, fmt.Errorf("not found %s.%s", SchemaName, data.TableName)
	}
	if fromPriKey == "" && len(data.Pri) > 0 {
		fromPriKey = data.Pri[0]
		toPriKey = data.Pri[0]
	}

	p := &PluginParam0{
		Field:          fieldList,
		PriKey:         priKeyList,
		SchemaName:     SchemaName,
		TableName:      data.TableName,
		SchemaAndTable: key,
		FromPriKey:     fromPriKey,
		ToPriKey:       toPriKey,
	}
	conn.p.tableMap[key] = p
	return p, nil
}

// 查出 目标库 里所有database,放到 map 中，用于缓存
func (conn *MysqlConn) initToDatabaseMap() {
	conn.p.toDatabaseMap = make(map[string]bool, 0)
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	SchemaList := conn.db.GetSchemaList()
	for _, Name := range SchemaList {
		conn.p.toDatabaseMap[Name] = true
	}
	return
}

func (conn *MysqlConn) initVersion() {
	defer func() {
		if err := recover(); err != nil {
			logrus.Println("plugin mysql initVersion recover:", err, string(debug.Stack()))
			return
		}
	}()
	if conn.db == nil {
		return
	}
	conn.serverVersion = conn.db.SelectVersion()
	if strings.Contains(conn.serverVersion, "TiDB") {
		conn.isTiDB = true
	}
}

func (conn *MysqlConn) Connect() bool {
	conn.db = NewMysqlDBConn(conn.uri)
	if conn.db.err == nil {
		conn.db.conn.Exec("SET NAMES utf8mb4", []dbDriver.Value{})
	}
	return true
}

func (conn *MysqlConn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			conn.db.err = fmt.Errorf(fmt.Sprint(err) + " debug:" + string(debug.Stack()))
			conn.err = conn.db.err
		}
	}()
	if conn.db != nil {
		conn.closeStmt0()
		conn.db.Close()
	}
	conn.Connect()
	if conn.db.err == nil {
		conn.initTableInfo()
	}
	return true
}

func (conn *MysqlConn) StmtClose() {
	for k, stmt := range conn.p.stmtArr {
		if stmt != nil {
			func() {
				defer func() {
					if err := recover(); err != nil {
						conn.db.err = fmt.Errorf("StmtClose err:%s", fmt.Sprint(err))
						return
					}
				}()
				stmt.Close()
			}()
		}
		conn.p.stmtArr[k] = nil
	}
}

func (conn *MysqlConn) Close() bool {
	if conn.db != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			conn.db.Close()
		}()
	}
	return true
}

func (conn *MysqlConn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	var n int
	if retry == false {
		conn.p.Data.Data = append(conn.p.Data.Data, data)
	}
	n = len(conn.p.Data.Data)
	if conn.p.BatchSize <= n {
		LastSuccessCommitData, ErrData, err = conn.AutoCommit()
		if LastSuccessCommitData != nil {
			conn.p.SkipBinlogData = nil
		}
		return
	}
	return nil, nil, nil
}

func (conn *MysqlConn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToCacheList(data, retry)
}

func (conn *MysqlConn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToCacheList(data, retry)
}

func (conn *MysqlConn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return conn.sendToCacheList(data, retry)
}

func (conn *MysqlConn) Query(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	if conn.p.AutoTable == false || data.Query == "" {
		return nil, nil, nil
	}
	switch data.Query {
	case "COMMIT", "commit", "BEGIN", "begin":
		return nil, nil, nil
	default:
		break
	}
	for {
		LastSuccessCommitData, ErrData, err = conn.AutoCommit()
		if err != nil {
			break
		}
		if len(conn.p.Data.Data) == 0 {
			if conn.CheckDataSkip(data) {
				conn.p.SkipBinlogData = nil
				return data, nil, nil
			}
			newSqlArr := conn.TranferQuerySql(data)
			if len(newSqlArr) == 0 {
				logrus.Println("transfer sql error!", data)
				return nil, data, fmt.Errorf("transfer sql error")
			}
			if conn.db.err != nil {
				conn.ReConnect()
			}
			if conn.db.err != nil {
				return nil, nil, conn.db.err
			}
			for _, newSql := range newSqlArr {
				if newSql == "" {
					continue
				}
				_, conn.db.err = conn.db.conn.Exec(newSql, []dbDriver.Value{})
				if conn.db.err != nil {
					logrus.Printf("plugin mysql, exec sql:%s err:%s", newSql, conn.db.err)
					return nil, data, conn.db.err
				}
			}
			break
		}
	}
	return
}

func (conn *MysqlConn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	n := len(conn.p.Data.Data)
	if n == 0 {
		return data, nil, nil
	}
	n0 := n / conn.p.BatchSize
	if len(conn.p.Data.CommitData)-1 < n0 {
		conn.p.Data.CommitData = append(conn.p.Data.CommitData, data)
	} else {
		conn.p.Data.CommitData[n0] = data
	}
	return nil, nil, nil
}

func (conn *MysqlConn) TimeOutCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	LastSuccessCommitData, ErrData, err = conn.AutoCommit()
	if LastSuccessCommitData != nil {
		conn.p.SkipBinlogData = nil
	}
	return
}

func (conn *MysqlConn) getMySQLData(data *pluginDriver.PluginDataType, index int, key string) interface{} {
	if key == "" {
		return nil
	}
	if _, ok := data.Rows[index][key]; ok {
		return data.Rows[index][key]
	}
	switch key {
	case "{$EventType}":
		return data.EventType
		break
	case "{$Timestamp}":
		return time.Now().Unix()
		break
	case "{$BinlogTimestamp}":
		return data.Timestamp
		break
	case "{$BinlogFileNum}":
		return data.BinlogFileNum
		break
	case "{$BinlogPosition}":
		return data.BinlogPosition
		break
	default:
		return pluginDriver.TransfeResult(key, data, index, true)
		break
	}
	return ""
}

// 设置跳过的位点
func (conn *MysqlConn) Skip(SkipData *pluginDriver.PluginDataType) error {
	conn.p.SkipBinlogData = SkipData
	return nil
}

func (conn *MysqlConn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(string(debug.Stack()))
			logrus.Println(string(debug.Stack()))
			conn.db.err = e
			conn.err = e
		}
	}()
	n := len(conn.p.Data.Data)
	if n == 0 {
		return nil, nil, nil
	}
	if conn.p.SyncMode == SYNCMODE_NO_SYNC_DATA {
		binlogEvent := conn.p.Data.CommitData[len(conn.p.Data.CommitData)-1]
		conn.p.Data = NewTableData()
		return binlogEvent, nil, nil
	}
	if conn.db.err != nil {
		conn.ReConnect()
	}
	if conn.db.err != nil {
		return nil, nil, conn.db.err
	}
	if n > conn.p.BatchSize {
		n = conn.p.BatchSize
	}
	list := conn.p.Data.Data[:n]
	if conn.p.AutoTable {
		ErrData, e = conn.AutoTableCommit(list)
	} else {
		ErrData, e = conn.NotAutoTableCommit(list)
	}
	if e != nil {
		logrus.Println("e:", e)
		if conn.p.BifrostMustBeSuccess {
			return nil, ErrData, e
		}
	}
	var binlogEvent *pluginDriver.PluginDataType
	if len(conn.p.Data.Data) <= int(conn.p.BatchSize) {
		if len(conn.p.Data.CommitData) > 1 {
			binlogEvent = conn.p.Data.CommitData[0]
		}
		conn.p.Data = NewTableData()
	} else {
		conn.p.Data.Data = conn.p.Data.Data[n:]
		if len(conn.p.Data.CommitData) > 0 {
			binlogEvent = conn.p.Data.CommitData[0]
			conn.p.Data.CommitData = conn.p.Data.CommitData[1:]
		}
	}
	return binlogEvent, nil, nil
}

func (conn *MysqlConn) NotAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	conn.db.err = conn.db.Begin()
	if conn.db.err != nil {
		return nil, conn.db.err
	}
	ErrData = conn.commitData(list)

	if conn.db.err != nil {
		conn.err = conn.db.err
		return ErrData, conn.err
	}
	if conn.err != nil {
		conn.db.err = conn.db.Rollback()
		logrus.Println("plugin mysql err", conn.err)
		return ErrData, conn.err
	}
	conn.db.err = conn.db.Commit()
	conn.StmtClose()
	if conn.db.err != nil {
		return nil, conn.db.err
	}
	return
}

// 自动创建表的提交
func (conn *MysqlConn) AutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, data := range dataMap {
		p, err := conn.CreateTableAndGetTableFieldsType(data[0])
		if err != nil {
			return data[0], err
		}
		if p == nil {
			continue
		}
		conn.p.Field = p.Field
		conn.p.fieldCount = len(p.Field)
		conn.p.schemaAndTable = p.SchemaAndTable
		conn.p.PriKey = p.PriKey
		conn.p.toPriKey = p.ToPriKey
		conn.p.fromPriKey = p.FromPriKey
		conn.db.err = conn.db.Begin()
		if conn.db.err != nil {
			conn.err = conn.db.err
			break
		}

		ErrData = conn.commitData(data)
		if conn.db.err != nil {
			conn.err = conn.db.err
		}
		if conn.err != nil {
			conn.db.err = conn.db.Rollback()
			logrus.Printf("[ERROR] output[%s] AutoTableCommit commitData err:%+v \n", OutputName, conn.err)
			return ErrData, conn.err
		}
		conn.db.err = conn.db.Commit()
		conn.StmtClose()
		if conn.db.err != nil {
			break
		}
	}
	return
}

func (conn *MysqlConn) commitData(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType) {
	switch conn.p.SyncMode {
	case SYNCMODE_NORMAL:
		if conn.IsStarRocks() {
			ErrData = conn.StarRocksCommitNormal(list)
		} else {
			ErrData = conn.CommitNormal(list)
		}
		break
	case SYNCMODE_LOG_UPDATE:
		if conn.IsStarRocks() {
			ErrData = conn.StarRocksCommit_Append(list)
		} else {
			ErrData = conn.CommitLogMod_Update(list)
		}
		break
	case SYNCMODE_LOG_APPEND:
		if conn.IsStarRocks() {
			ErrData = conn.StarRocksCommit_Append(list)
		} else {
			ErrData = conn.CommitLogMod_Append(list)
		}
		break
	default:
		conn.err = fmt.Errorf("同步模式ERROR:%s", conn.p.SyncMode)
		break
	}
	return
}

func (conn *MysqlConn) dataTypeTransfer(data interface{}, fieldName string, toDataType string, defaultVal *string) (v dbDriver.Value, e error) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Printf("[ERROR] output[%s] dataTypeTransfer pacnic:%+v stack:%+v \n", OutputName, err, string(debug.Stack()))
			e = fmt.Errorf(fieldName + " " + fmt.Sprint(err))
		}
	}()
	if data == nil {
		if conn.p.NullTransferDefault == false {
			if defaultVal == nil {
				v = nil
				return
			} else {
				// 这里要判断 是不是 bit 类型，因为 bit 类型在传输值 上 必须 为 int64 类型，不能是字符串
				if toDataType == "bit" {
					v, _ = strconv.ParseInt(*defaultVal, 10, 64)
				} else {
					v = *defaultVal
				}
				return
			}
		} else {
			//假如配置是强制转成默认值
			switch toDataType {
			case "int", "tinyint", "smallint", "mediumint", "bigint", "bool":
				v = "0"
				break
			case "bit":
				v = int64(0)
				break
			case "date":
				v = "1970-01-01"
				break
			case "timestamp":
				v = "1970-01-01 00:00:01"
				break
			case "datetime":
				v = "1000-01-01 00:00:00"
				break
			case "time":
				v = "00:00:01"
				break
			case "year":
				v = "1970"
				break
			case "float", "double", "decimal", "number", "point":
				v = "0.00"
				break
			case "json":
				v = "{}"
				break
			default:
				v = ""
				break
			}
			return
		}
	}
	switch data.(type) {
	case bool:
		if data.(bool) == false {
			data = "0"
		} else {
			data = "1"
		}
		break
	default:
		break
	}
	switch toDataType {
	case "bool":
		switch fmt.Sprint(data) {
		case "0", "":
			v = "0"
			break
		default:
			v = "1"
		}
		break
	case "bit":
		switch data.(type) {
		case string:
			v, _ = strconv.ParseInt(data.(string), 10, 64)
			break
		case int64:
			v = data.(int64)
		case float64:
			v = int64(data.(float64))
		case float32:
			v = int64(data.(float32))
		default:
			v, _ = strconv.ParseInt(fmt.Sprint(data), 10, 64)
			break
		}
		break
	case "set":
		switch data.(type) {
		case []string, []interface{}:
			v = strings.Replace(strings.Trim(fmt.Sprint(data), "[]"), " ", ",", -1)
			break
		default:
			v = fmt.Sprint(data)
			break
		}
		break
	case "json":
		switch reflect.TypeOf(data).Kind() {
		case reflect.Array, reflect.Slice, reflect.Map:
			var c []byte
			c, e = json.Marshal(data)
			if e != nil {
				return
			}
			v = string(c)
			break
		case reflect.String:
			v = data
		default:
			e = fmt.Errorf("field:%s ,data source type: %s, is not object or array, s ", fieldName, reflect.TypeOf(data).Kind().String())
		}
		break
	default:
		v, e = conn.data2String(data)
		if e != nil {
			e = fmt.Errorf("field:%s ,%s", fieldName, e.Error())
		}
		break
	}
	return
}

func (conn *MysqlConn) data2String(data interface{}) (v string, e error) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.String:
		switch data.(type) {
		case json.Number:
			return data.(json.Number).String(), nil
		default:
			return fmt.Sprint(data), nil
		}
	case reflect.Array, reflect.Slice, reflect.Map:
		var c []byte
		c, e = json.Marshal(data)
		if e != nil {
			e = fmt.Errorf("data source type: %s , json.Marshal err: %s ", reflect.TypeOf(data).Kind().String(), e.Error())
			return
		}
		v = string(c)
		break
	case reflect.Float32:
		v = strconv.FormatFloat(float64(data.(float32)), 'E', -1, 32)
	case reflect.Float64:
		v = strconv.FormatFloat(data.(float64), 'E', -1, 64)
	default:
		v = fmt.Sprint(data)
	}
	return
}

func (conn *MysqlConn) getStmt(Type EventType) dbDriver.Stmt {
	if conn.p.stmtArr[Type] != nil {
		return conn.p.stmtArr[Type]
	}
	switch Type {
	case REPLACE_INSERT:
		fields := ""
		values := ""
		for _, v := range conn.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
			}
		}
		sql := "REPLACE INTO " + conn.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ")"
		conn.p.stmtArr[Type], conn.db.err = conn.db.conn.Prepare(sql)
		if conn.db.err != nil {
			logrus.Println("mysql getStmt REPLACE_INSERT err:", conn.db.err, sql)
		}
		break
	case INSERT:
		fields := ""
		values := ""
		for _, v := range conn.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
			}
		}
		sql := "INSERT INTO " + conn.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ")"
		conn.p.stmtArr[Type], conn.db.err = conn.db.conn.Prepare(sql)
		if conn.db.err != nil {
			logrus.Println("mysql getStmt INSERT err:", conn.db.err, sql)
		}
		break
	case DELETE:
		where := ""
		for _, v := range conn.p.PriKey {
			if where == "" {
				where = "`" + v.ToField + "`=?"
			} else {
				where += " AND `" + v.ToField + "`=?"
			}
		}
		conn.p.stmtArr[Type], conn.db.err = conn.db.conn.Prepare("DELETE FROM " + conn.p.schemaAndTable + " WHERE " + where)
		if conn.db.err != nil {
			logrus.Println("mysql getStmt DELETE err:", conn.db.err)
		}
		break
	case UPDATE:
		fields := ""
		values := ""
		fields2 := ""
		for _, v := range conn.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
				fields2 = "`" + v.ToField + "`=?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
				fields2 += ",`" + v.ToField + "`=?"
			}
		}
		sql := "INSERT INTO " + conn.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ") ON DUPLICATE KEY UPDATE " + fields2
		conn.p.stmtArr[Type], conn.db.err = conn.db.conn.Prepare(sql)
		if conn.db.err != nil {
			logrus.Println("mysql getStmt INSERT ON DUPLICATE KEY UPDATE err:", conn.db.err, sql)
		}
		break
	}

	return conn.p.stmtArr[Type]
}

func (conn *MysqlConn) closeStmt0() {
	for k, _ := range conn.p.stmtArr {
		conn.p.stmtArr[k] = nil
	}
}

func (conn *MysqlConn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if conn.p.SkipBinlogData != nil && conn.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && conn.p.SkipBinlogData.BinlogPosition == data.BinlogPosition {
		if conn.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && conn.p.SkipBinlogData.BinlogPosition >= data.BinlogPosition {
			return true
		}
		if conn.p.SkipBinlogData.BinlogFileNum > data.BinlogFileNum {
			return true
		}
	}
	return false
}

func checkOpMap(opMap map[interface{}]*opLog, key interface{}, EvenType string) bool {
	if key == "" {
		return false
	}
	if _, ok := opMap[key]; ok {
		return true
	}
	return false
}

func setOpMapVal(opMap map[interface{}]*opLog, key interface{}, data *[]dbDriver.Value, EventType string) {
	opMap[key] = &opLog{Data: data, EventType: EventType}
}
