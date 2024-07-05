package proxy

import (
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"strconv"
	"strings"
)

const (
	TYPE_BIG_REQUEST         = 1        //Big Request Definition：Operated multiple key;Operated on multiple members of key;The value of the key or a member of the operation is too large
	TYPE_BIG_KEY             = 2        //Big Key Definition：key has too many members;The data on the key is large; BigKey generally based on the response
	MAX_VALUE_LENGTH_DEFAULT = 2 * 1024 //default BigValue definition

	MAX_BATCHSIZE_DEFAULT = 100 //default upper limit of argument

	MAX_RESULT_SET_SIZE_DEFAULT  = 20  //default result set size is 20
	MAX_RESULT_SET_SIZE_BOUNDARY = 500 //No more than 500
)

var (
	checker = &Checker{}
	MaxValueLength   atomic2.Int64
	MaxBatchsize     atomic2.Int64
	MaxResultSetSize atomic2.Int64
)

type Checker struct {
	Enabled atomic2.Int64
}

func IsCheckerEnable() bool {
	return checker.Enabled.Int64() == 1
}
func SetCheckerState(state int64) {
	if state != 0 && state != 1 { //The passed argument is not 1 or 0
		state = 0 //Set the value to 0 forcibly
	}
	checker.Enabled.Set(state)
}

func CheckerSetMaxLengthOfValue(maxLength int64) {
	if maxLength < 0 {
		SetMaxValueLength(MAX_VALUE_LENGTH_DEFAULT)
	} else {
		SetMaxValueLength(maxLength)
	}
}
func CheckerSetMaxBatchsize(maxBatchsize int64) {
	if maxBatchsize < 0 {
		SetMaxBatchSize(MAX_BATCHSIZE_DEFAULT)
	} else {
		SetMaxBatchSize(maxBatchsize)
	}
}

func CheckerSetResultSetSize(resultSetLimitation int64) {
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
func SetMaxResultSetSize(maxResultSetSize int64) {
	MaxResultSetSize.Set(maxResultSetSize)
}
func GetMaxResultSetSize() int64 {
	return MaxResultSetSize.Int64()
}

func (f OpFlagChecker) CheckerRequest(r *Request) bool {
	var isBigRequest bool = false
	if f.NeedCheckBatchsizeOfRequest() {
		isBigRequest = CheckBatchsizeOfRequest(r, f)
	} else if f.NeedCheckContentOfRequest() {
		isBigRequest = CheckContentOfRequest(r, f)
	}
	return isBigRequest
}
func (f OpFlagChecker) MonitorResponse(r *Request, remoteAddr string) bool {
	if len(r.Multi) < 2 {
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)

	if f.NeedCheckNumberOfResp() {
		return CheckResponseByNumberReturned(r, f, string(key))
	} else if f.NeedCheckSingleValueOfResp() {
		return CheckResponseByContentReturned(r, string(key))
	} else if f.NeedCheckArrayOfResp() {
		return CheckResponseByArrayReturned(r, f, string(key))
	}
	return false
}

// Request part
func CheckBatchsizeOfRequest(r *Request, flag OpFlagChecker) bool {
	if len(r.Multi) < 2 {
		return false
	}
	var keyNum int64
	var fieldNum int64
	var keyList = make([]string, 0)

	switch flag & (FlagReqKeys | FlagReqKeyFields) {
	case FlagReqKeys:
		// CMD key1 ~ keyN                  1 + N
		keyNum = int64(len(r.Multi) - 1)
		for i := 1; i < len(r.Multi); i++ {
			if int64(len(keyList)) >= GetMaxResultSetSize() {
				break
			}
			keyList = append(keyList, string(r.Multi[i].Value))
		}
	case FlagReqKeyFields:
		// CMD KEY field1 ~ fieldN          1 + 1 + N
		fieldNum = int64(len(r.Multi) - 2)
		keyNum = 1
	}
	if fieldNum >= GetMaxBatchSize() || keyNum >= GetMaxBatchSize() {
		return true
	}
	return false
}

func CheckContentOfRequest(r *Request, flag OpFlagChecker) bool {
	var hasBigValue bool = false
	var keyNum int64 = 0
	var fieldNum int64 = 0
	var bigkeys = make([]string, 0)

	switch flag & (FlagReqValues | FlagReqKeyValues | FlagReqKeyFieldValues | FlagReqKeyTtlValue) {
	case FlagReqValues:
		// CMD Key [value1]...[valueN]    N>=1
		nvs := len(r.Multi) - 2 // num of values
		if nvs <= 0 {
			return false
		}
		key := getHashKey(r.Multi, r.OpStr)
		fieldNum = int64(nvs)
		keyNum = 1
		var (
			Key           string = string(key)
			MaxMemberSize int64  = 0
		)
		for i := 0; i < nvs; i++ {
			valuelength := int64(len(r.Multi[i+2].Value))
			if valuelength >= GetMaxValueLength() {
				hasBigValue = true
			}
			if valuelength >= MaxMemberSize {
				MaxMemberSize = valuelength
			}
		}
		bigkeys = append(bigkeys, Key)
	case FlagReqKeyValues:
		// CMD key1 [value1] ... keyN [valueN]
		nkvs := len(r.Multi) - 1
		if nkvs <= 0 || nkvs%2 != 0 {
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
				bigkeys = append(bigkeys, key)
			}
		}
	case FlagReqKeyFieldValues:
		// CMD Key field1 [value1] ... fieldN [valueN]
		nkfvs := len(r.Multi) - 2
		if nkfvs <= 0 || nkfvs%2 != 0 {
			return false
		}
		keyNum = 1
		var (
			Key           string = string(r.Multi[1].Value)
			MaxMemberSize int64  = 0
		)
		for i := 0; i < nkfvs/2; i++ {
			valuesize := int64(len(r.Multi[i*2+3].Value))
			if MaxMemberSize <= valuesize {
				MaxMemberSize = valuesize
			}
			if valuesize >= GetMaxValueLength() {
				hasBigValue = true
			}
		}
		bigkeys = append(bigkeys, Key)
	case FlagReqKeyTtlValue:
		// CMD Key ttl value
		// only one key，Batchsize and MaxMemberSize do not need to be recorded
		if len(r.Multi) != 4 {
			return false
		}
		key := getHashKey(r.Multi, r.OpStr)
		valuelength := int64(len(r.Multi[3].Value))
		fieldNum = 1
		keyNum = 1
		var Key string = string(key)
		if valuelength >= GetMaxValueLength() {
			hasBigValue = true
		}

		bigkeys = append(bigkeys, Key)
	}

	if hasBigValue || keyNum >= GetMaxBatchSize() || fieldNum >= GetMaxBatchSize() {
		StoreKeyBlackLists(strings.Join(bigkeys, ","))
		return true
	}
	return false
}

// Response part
func CheckResponseByNumberReturned(r *Request, flag OpFlagChecker, key string) bool {
	var contentlength int = 0
	var isContentTooLarge bool = false

	if r.Resp.IsInt() {
		res, err := strconv.Atoi(string(r.Resp.Value))
		if err != nil {
			return true
		}
		contentlength = res
	}
	switch flag & (FlagRespReturnArraysize | FlagRespReturnValuesize) {
	case FlagRespReturnArraysize:
		isContentTooLarge = int64(contentlength) >= GetMaxBatchSize()
	case FlagRespReturnValuesize:
		isContentTooLarge = int64(contentlength) >= GetMaxValueLength()
	}
	if isContentTooLarge {
		StoreKeyBlackList(key)
	}
	return true
}

func CheckResponseByContentReturned(r *Request, key string) bool {
	var isContentTooLarge bool = false
	if r.Resp.IsBulkBytes() {
		isContentTooLarge = int64(len(r.Resp.Value)) >= GetMaxValueLength()
	}
	if isContentTooLarge {
		StoreKeyBlackList(key)
	}
	return isContentTooLarge
}

func CheckResponseByArrayReturned(r *Request, flag OpFlagChecker, key string) bool {
	var hasBigValue, batchTooLarge bool
	var bigkeys = make([]string, 0)

	if r.Resp.IsArray() {
		switch flag & (FlagRespReturnArray | FlagRespReturnArrayByPair | FlagRespCheckArrayLength | FlagRespCheckArrayLengthByPair) {
		case FlagRespReturnArray:
			var (
				DataSize      int64 = 0
				MaxMemberSize int64 = 0
			)
			batchTooLarge = int64(len(r.Resp.Array)) >= GetMaxBatchSize()
			for _, item := range r.Resp.Array {
				if item.IsBulkBytes() {
					DataSize += int64(len(item.Value))
					if int64(len(item.Value)) >= MaxMemberSize {
						MaxMemberSize = int64(len(item.Value))
					}
					if int64(len(item.Value)) >= GetMaxValueLength() {
						hasBigValue = true
					}
				}
			}
			if DataSize >= GetMaxValueLength() {
				hasBigValue = true
			}
			bigkeys = append(bigkeys, key)
		case FlagRespReturnArrayByPair:
			arrLen := len(r.Resp.Array)
			if arrLen%2 != 0 { //Two in a group, so must be an integer multiple of 2, otherwise ignore
				return false
			}
			var (
				DataSize      int64 = 0
				MaxMemberSize int64 = 0
				Batchsize     int64 = int64(arrLen / 2)
			)
			batchTooLarge = Batchsize >= GetMaxBatchSize()
			for i := 0; i < arrLen/2; i++ {
				if r.Resp.Array[i*2].IsBulkBytes() && r.Resp.Array[i*2+1].IsBulkBytes() {
					valueLen := int64(len(r.Resp.Array[i*2+1].Value))
					DataSize += valueLen
					if MaxMemberSize <= valueLen {
						MaxMemberSize = valueLen
					}
					if valueLen >= GetMaxValueLength() {
						hasBigValue = true
					}
				}
			}
			if DataSize >= GetMaxValueLength() {
				hasBigValue = true
			}
			bigkeys = append(bigkeys, key)
		case FlagRespCheckArrayLength:

			var Batchsize = int64(len(r.Resp.Array))
			batchTooLarge = Batchsize >= GetMaxBatchSize()
			bigkeys = append(bigkeys, key)
		case FlagRespCheckArrayLengthByPair:
			arrLen := int64(len(r.Resp.Array))
			if arrLen%2 != 0 { //Two in a group, so must be an integer multiple of 2, otherwise ignore
				return false
			}
			var Batchsize = arrLen / 2
			batchTooLarge = Batchsize >= GetMaxBatchSize()
			bigkeys = append(bigkeys, key)
		}
	}
	if hasBigValue || batchTooLarge {
		StoreKeyBlackLists(strings.Join(bigkeys, ","))
	}
	return hasBigValue || batchTooLarge
}
