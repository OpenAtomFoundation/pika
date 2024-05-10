package proxy

import (
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"strconv"
	"time"
)

const (
	TYPE_BIG_REQUEST  = 1 //大请求定义：操作了多个key；操作了key的多个成员；操作的key或者某个成员的值过大
	TYPE_BIG_RESPONSE = 2 //大响应定义：此类定义比较特殊，一般返回的是数组，但数组并不指代任何key，从请求也看不出异常。定义为大响应一般是响应数组过大或者数据量过大;此类命令类似于sinter,sdiff,sunion等
	TYPE_BIG_KEY      = 3 //大key定义：key的成员过多；key本身数据量过大。大key一般根据响应得出
	TYPE_HIGH_RISK    = 4 //高风险操作：本身操作包含一定风险，带有不确定性

	MAX_VALUE_LENGTH_DEFAULT = 2 * 1024 //默认2k算大value

	MAX_BATCHSIZE_DEFAULT = 100 //默认参数上限为100

	MAX_CMD_INFO_LENGTH_DEFAULT        = 64  //默认记录redis命令的上限为64个字节，最少不得少于64个字节
	MAX_CMD_INFO_LENGTH_UPPER_BOUNDARY = 512 //最多不得超过512个字节

	MAX_RESULT_SET_SIZE_DEFAULT  = 20  //默认xmonitor结果集的大小为20
	MAX_RESULT_SET_SIZE_BOUNDARY = 500 //最多不得超过500
)

var (
	monitor          = &Monitor{}
	MaxValueLength   atomic2.Int64 //最大的value限制，超出则被监控
	MaxBatchsize     atomic2.Int64 //最大的参数数量，超出则被监控
	MaxCmdInfoLength atomic2.Int64 //日志记录中记录请求的原始信息，最大为256个字节，超出则忽略
	MaxResultSetSize atomic2.Int64 //结果集数量限制
)

type Monitor struct {
	Enabled atomic2.Int64
}

func IsMonitorEnable() bool {
	return monitor.Enabled.Int64() == 1
}
func XMonitorSetMonitorState(state int64) {
	if state != 0 && state != 1 { //传入参数不是1或0
		state = 0 //强制置为0，即关闭
	}
	monitor.Enabled.Set(state)
}

func XMonitorSetMaxLengthOfValue(maxLength int64) {
	if maxLength < 0 {
		SetMaxValueLength(MAX_VALUE_LENGTH_DEFAULT)
	} else {
		SetMaxValueLength(maxLength)
	}
}
func XMonitorSetMaxBatchsize(maxBatchsize int64) {
	if maxBatchsize < 0 {
		SetMaxBatchSize(MAX_BATCHSIZE_DEFAULT)
	} else {
		SetMaxBatchSize(maxBatchsize)
	}
}
func XMonitorSetMaxLengthOfCmdInfo(maxLength int64) {
	if maxLength < MAX_CMD_INFO_LENGTH_DEFAULT {
		SetMaxCmdInfoLen(MAX_CMD_INFO_LENGTH_DEFAULT)
	} else if maxLength >= MAX_CMD_INFO_LENGTH_UPPER_BOUNDARY {
		SetMaxCmdInfoLen(MAX_CMD_INFO_LENGTH_UPPER_BOUNDARY)
	} else {
		SetMaxCmdInfoLen(maxLength)
	}
}
func XMonitorSetResultSetSize(resultSetLimitation int64) {
	if resultSetLimitation < 0 {
		SetMaxResultSetSize(MAX_RESULT_SET_SIZE_DEFAULT)
	} else if resultSetLimitation >= MAX_RESULT_SET_SIZE_BOUNDARY {
		SetMaxResultSetSize(MAX_RESULT_SET_SIZE_BOUNDARY)
	} else {
		SetMaxResultSetSize(resultSetLimitation)
	}
}

func SetMaxValueLength(maxValueLength int64) {
	MaxValueLength.Set(maxValueLength)
}
func GetMaxValueLength() int64 {
	return MaxValueLength.Int64()
}
func SetMaxBatchSize(maxBatchsize int64) {
	MaxBatchsize.Set(maxBatchsize)
}
func GetMaxBatchSize() int64 {
	return MaxBatchsize.Int64()
}
func SetMaxCmdInfoLen(maxCmdInfoLen int64) {
	MaxCmdInfoLength.Set(maxCmdInfoLen)
}
func GetMaxCmdInfoLen() int64 {
	return MaxCmdInfoLength.Int64()
}
func SetMaxResultSetSize(maxResultSetSize int64) {
	MaxResultSetSize.Set(maxResultSetSize)
}
func GetMaxResultSetSize() int64 {
	return MaxResultSetSize.Int64()
}

type KeyInfo struct {
	Key           string `json:"key"`
	Batchsize     int64  `json:"batchsize,omitempty"`    //针对数据类型为：[list，set，zset，hash]，被涉及到的成员数量
	MaxMemberSize int64  `json:"max_mem_size,omitempty"` //针对数据类型为hash的情况，对应涉及到成员中，最大的成员大小
	DataSize      int64  `json:"datasize,omitempty"`     //针对数据类型为string的字节长度，以及数据类型为hash的操作的成员总字节长度（该长度可能不是key的总大小）
}

type KeyDetail struct {
	NumOfOperatedKeys int64     `json:"key_num,omitempty"`
	KeyInfoList       []KeyInfo `json:"keys_info,omitempty"`
	ResponseBatchsize int64     `json:"response_batchsize,omitempty"` //特殊情况，针对redis响应集合的大小
}

type Record struct {
	TimeStamp    string    `json:"ts"`
	CmdName      string    `json:"cmd_name"`
	Cmdinfo      string    `json:"cmd_info"`
	RemoteAddr   string    `json:"remote_addr"`
	AbnormalType int       `json:"type"`
	Details      KeyDetail `json:"details,omitempty"`
	DelayUs      int64     `json:"delay_us,omitempty"`
}

func NewRecord(r *Request, remoteAddr string) *Record {
	var cmdInfo = make([]byte, GetMaxCmdInfoLen())
	index := getWholeCmd(r.Multi, cmdInfo)
	now := time.Now()
	return &Record{
		TimeStamp:  now.Format(time.RFC3339Nano),
		CmdName:    r.OpStr,
		Cmdinfo:    string(cmdInfo[:index]),
		RemoteAddr: remoteAddr,
		Details:    KeyDetail{KeyInfoList: make([]KeyInfo, 0)},
	}
}

func collectRecord(record *Record) {
	//直接将record结构体存入monitorLog，查询时再转换成字符串
	MonitorLogPushBack(&MlogEntry{id: MonitorLogGetCurId(), log: record})
}

func (f OpFlagMonitor) MonitorRequest(r *Request, remoteAddr string) bool {
	var isBigRequest bool = false
	if f.NeedCheckBatchsizeOfRequest() {
		isBigRequest = CheckBatchsizeOfRequest(r, f, remoteAddr)
	} else if f.NeedCheckContentOfRequest() {
		isBigRequest = CheckContentOfRequest(r, f, remoteAddr)
	}
	if f.IsHighRisk() { //请求阶段会进行高危命令判断，如果是高危命令，不管参数是什么，一律记录
		RecordHighRiskCMD(r, remoteAddr)
	}
	return isBigRequest
}

func (f OpFlagMonitor) MonitorResponse(r *Request, remoteAddr string, delay int64) bool {
	if len(r.Multi) < 2 {
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)

	if f.NeedCheckNumberOfResp() {
		return CheckResponseByNumberReturned(r, f, string(key), remoteAddr, delay)
	} else if f.NeedCheckSingleValueOfResp() {
		return CheckResponseByContentReturned(r, string(key), remoteAddr, delay)
	} else if f.NeedCheckArrayOfResp() {
		return CheckResponseByArrayReturned(r, f, string(key), remoteAddr, delay)
	}
	return false
}

// Request part
func RecordHighRiskCMD(r *Request, remoteAddr string) bool {
	var record = NewRecord(r, remoteAddr)
	record.AbnormalType = TYPE_HIGH_RISK
	collectRecord(record)
	return true
}

func CheckBatchsizeOfRequest(r *Request, flag OpFlagMonitor, remoteAddr string) bool {
	if len(r.Multi) < 2 {
		return false
	}
	var keyNum int64
	var fieldNum int64
	var keyinfoList = make([]KeyInfo, 0)

	switch flag & (FlagReqKeys | FlagReqKeyFields) {
	case FlagReqKeys:
		// CMD key1 ~ keyN                  1 + N
		keyNum = int64(len(r.Multi) - 1)
		for i := 1; i < len(r.Multi); i++ {
			if int64(len(keyinfoList)) >= GetMaxResultSetSize() {
				break
			}
			keyinfoList = append(keyinfoList, KeyInfo{
				Key: string(r.Multi[i].Value),
			})
		}
	case FlagReqKeyFields:
		// CMD KEY field1 ~ fieldN          1 + 1 + N
		fieldNum = int64(len(r.Multi) - 2)
		keyNum = 1
		keyinfoList = append(keyinfoList, KeyInfo{
			Key:       string(r.Multi[1].Value),
			Batchsize: fieldNum,
		})
	}
	if fieldNum >= GetMaxBatchSize() || keyNum >= GetMaxBatchSize() {
		var record = NewRecord(r, remoteAddr)
		record.Details.NumOfOperatedKeys = keyNum
		record.Details.KeyInfoList = keyinfoList
		record.AbnormalType = TYPE_BIG_REQUEST
		collectRecord(record)

		return true
	}

	return false
}

func CheckContentOfRequest(r *Request, flag OpFlagMonitor, remoteAddr string) bool {
	var hasBigValue bool = false
	var keyNum int64 = 0
	var fieldNum int64 = 0
	var bigkeys = make([]KeyInfo, 0)

	switch flag & (FlagReqValues | FlagReqKeyValues | FlagReqKeyFieldValues | FlagReqKeyTtlValue) {
	case FlagReqValues:
		// CMD Key [value1]...[valueN]    N>=1
		nvs := len(r.Multi) - 2 // num of values
		if nvs <= 0 {           //非正确请求，跳过监控
			return false
		}
		key := getHashKey(r.Multi, r.OpStr)
		fieldNum = int64(nvs)
		keyNum = 1
		var keyinfo = KeyInfo{
			Key:           string(key),
			Batchsize:     int64(nvs),
			MaxMemberSize: 0,
		}
		for i := 0; i < nvs; i++ {
			valuelength := int64(len(r.Multi[i+2].Value))
			keyinfo.DataSize += valuelength
			if valuelength >= GetMaxValueLength() {
				hasBigValue = true
			}
			if valuelength >= keyinfo.MaxMemberSize {
				keyinfo.MaxMemberSize = valuelength
			}
		}

		bigkeys = append(bigkeys, keyinfo)
	case FlagReqKeyValues:
		// CMD key1 [value1] ... keyN [valueN]
		nkvs := len(r.Multi) - 1
		if nkvs <= 0 || nkvs%2 != 0 { //非正确请求，跳过监控
			return false
		}
		keyNum = int64(nkvs / 2)
		for i := 0; i < nkvs/2; i++ {
			if int64(len(bigkeys)) >= GetMaxResultSetSize() {
				break
			}
			key := string(r.Multi[i*2+1].Value)
			valuelength := len(r.Multi[i*2+2].Value)
			if int64(valuelength) >= GetMaxValueLength() {
				hasBigValue = true
				bigkeys = append(bigkeys, KeyInfo{Key: key, DataSize: int64(valuelength)})
			}
		}
	case FlagReqKeyFieldValues:
		// CMD Key field1 [value1] ... fieldN [valueN]
		nkfvs := len(r.Multi) - 2
		if nkfvs <= 0 || nkfvs%2 != 0 { //非正确请求，跳过监控
			return false
		}
		fieldNum = int64(nkfvs / 2)
		keyNum = 1
		var keyinfo = KeyInfo{
			Key:           string(r.Multi[1].Value),
			Batchsize:     fieldNum,
			DataSize:      0,
			MaxMemberSize: 0,
		}
		for i := 0; i < nkfvs/2; i++ {
			valuesize := int64(len(r.Multi[i*2+3].Value))
			keyinfo.DataSize += valuesize
			if keyinfo.MaxMemberSize <= valuesize {
				keyinfo.MaxMemberSize = valuesize
			}
			if valuesize >= GetMaxValueLength() {
				hasBigValue = true
			}
		}
		bigkeys = append(bigkeys, keyinfo)
	case FlagReqKeyTtlValue:
		// CMD Key ttl value
		// 只有单个key，不需要记录Batchsize和MaxMemberSize
		if len(r.Multi) != 4 { //非正确请求，跳过监控
			return false
		}
		key := getHashKey(r.Multi, r.OpStr)
		valuelength := int64(len(r.Multi[3].Value))
		fieldNum = 1
		keyNum = 1
		var keyinfo = KeyInfo{
			Key:      string(key),
			DataSize: int64(valuelength),
		}

		if valuelength >= GetMaxValueLength() {
			hasBigValue = true
		}

		bigkeys = append(bigkeys, keyinfo)
	}

	if hasBigValue || keyNum >= GetMaxBatchSize() || fieldNum >= GetMaxBatchSize() {
		var record = NewRecord(r, remoteAddr)
		record.AbnormalType = TYPE_BIG_REQUEST
		record.Details.NumOfOperatedKeys = keyNum
		record.Details.KeyInfoList = bigkeys
		collectRecord(record)

		return true
	}

	return false
}

// Response part
func CheckResponseByNumberReturned(r *Request, flag OpFlagMonitor, key, remoteAddr string, delay int64) bool {
	var contentlength int = 0
	var isContentTooLarge bool = false

	if r.Resp.IsInt() {
		res, err := strconv.Atoi(string(r.Resp.Value))
		if err != nil {
			return true
		}
		contentlength = res
	}
	var keyinfo = KeyInfo{
		Key: key,
	}
	switch flag & (FlagRespReturnArraysize | FlagRespReturnValuesize) {
	case FlagRespReturnArraysize:
		isContentTooLarge = int64(contentlength) >= GetMaxBatchSize()
		keyinfo.Batchsize = int64(contentlength)
	case FlagRespReturnValuesize:
		isContentTooLarge = int64(contentlength) >= GetMaxValueLength()
		keyinfo.DataSize = int64(contentlength)
	}
	if isContentTooLarge {
		var record = NewRecord(r, remoteAddr)
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

func CheckResponseByContentReturned(r *Request, key, remoteAddr string, delay int64) bool {
	var isContentTooLarge bool = false
	var keyinfo = KeyInfo{
		Key: key,
	}
	if r.Resp.IsBulkBytes() {
		isContentTooLarge = int64(len(r.Resp.Value)) >= GetMaxValueLength()
	}
	if isContentTooLarge {
		var record = NewRecord(r, remoteAddr)
		record.AbnormalType = TYPE_BIG_KEY
		keyinfo.DataSize = int64(len(r.Resp.Value))
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		record.DelayUs = delay
		collectRecord(record)
	}
	return isContentTooLarge
}

func CheckResponseByArrayReturned(r *Request, flag OpFlagMonitor, key, remoteAddr string, delay int64) bool {
	var hasBigValue, batchTooLarge bool
	var keyNum int64
	var bigkeys = make([]KeyInfo, 0)

	if r.Resp.IsArray() {
		switch flag & (FlagRespReturnArray | FlagRespReturnArrayByPair | FlagRespCheckArrayLength | FlagRespCheckArrayLengthByPair) {
		case FlagRespReturnArray:
			bigkey := KeyInfo{
				Key:           key,
				Batchsize:     int64(len(r.Resp.Array)),
				MaxMemberSize: 0,
				DataSize:      0,
			}
			keyNum = 1
			batchTooLarge = int64(len(r.Resp.Array)) >= GetMaxBatchSize()
			for _, item := range r.Resp.Array {
				if item.IsBulkBytes() {
					bigkey.DataSize += int64(len(item.Value))
					if int64(len(item.Value)) >= bigkey.MaxMemberSize {
						bigkey.MaxMemberSize = int64(len(item.Value))
					}
					if int64(len(item.Value)) >= GetMaxValueLength() {
						hasBigValue = true
					}
				}
			}
			if bigkey.DataSize >= GetMaxValueLength() {
				hasBigValue = true
			}
			bigkeys = append(bigkeys, bigkey)
		case FlagRespReturnArrayByPair:
			arrLen := len(r.Resp.Array)
			if arrLen%2 != 0 { //两个为一组，所以必须是2的整数倍，否则忽略
				return false
			}
			bigkey := KeyInfo{
				Key:           key,
				Batchsize:     int64(arrLen / 2),
				MaxMemberSize: 0,
				DataSize:      0,
			}
			keyNum = 1
			batchTooLarge = bigkey.Batchsize >= GetMaxBatchSize()
			for i := 0; i < arrLen/2; i++ {
				if r.Resp.Array[i*2].IsBulkBytes() && r.Resp.Array[i*2+1].IsBulkBytes() {
					valueLen := int64(len(r.Resp.Array[i*2+1].Value))
					bigkey.DataSize += valueLen
					if bigkey.MaxMemberSize <= valueLen {
						bigkey.MaxMemberSize = valueLen
					}
					if valueLen >= GetMaxValueLength() {
						hasBigValue = true
					}
				}
			}
			if bigkey.DataSize >= GetMaxValueLength() {
				hasBigValue = true
			}
			bigkeys = append(bigkeys, bigkey)
		case FlagRespCheckArrayLength:
			var bigkey = KeyInfo{
				Key:       key,
				Batchsize: int64(len(r.Resp.Array)),
			}
			keyNum = 1
			batchTooLarge = bigkey.Batchsize >= GetMaxBatchSize()
			bigkeys = append(bigkeys, bigkey)
		case FlagRespCheckArrayLengthByPair:
			arrLen := int64(len(r.Resp.Array))
			if arrLen%2 != 0 { //两个为一组，所以必须是2的整数倍，否则忽略
				return false
			}
			var bigkey = KeyInfo{
				Key:       key,
				Batchsize: arrLen / 2,
			}
			keyNum = 1
			batchTooLarge = bigkey.Batchsize >= GetMaxBatchSize()
			bigkeys = append(bigkeys, bigkey)
		}
	}
	if hasBigValue || batchTooLarge {
		var record = NewRecord(r, remoteAddr)
		record.Details.NumOfOperatedKeys = keyNum
		record.Details.KeyInfoList = bigkeys
		record.AbnormalType = TYPE_BIG_KEY
		record.DelayUs = delay
		collectRecord(record)
	}
	return hasBigValue || batchTooLarge
}
