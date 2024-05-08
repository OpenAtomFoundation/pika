package proxy

import (
	"strconv"
	"strings"
)

// CMD: SET
/*
   说明：从 Redis 2.6.12 版本开始， SET 命令可追加参数进行操作，因此 SET KEY VALUE 之后可能会追加参数，因此
   SET命令做特殊化处理
*/
type CheckSET struct {}
func (c *CheckSET) CheckRequest (r *Request, s *Session) bool{
	if len(r.Multi) < 3 { //少于3个值，非正常set命令
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	if r.Multi[2].IsBulkBytes() {
		if int64(len(r.Multi[2].Value)) >= GetMaxValueLength() {
			var record = NewRecord(r, s.Conn.RemoteAddr())
			record.AbnormalType = TYPE_BIG_REQUEST
			record.Details.NumOfOperatedKeys = 1
			record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
				Key: string(key),
				DataSize: int64(len(r.Multi[2].Value)),
			})
			collectRecord(record)
			return true
		}
	}
	return false
}
func (c *CheckSET) CheckResponse (r *Request, s *Session, delay int64) bool{
	return false
}

// CMD: MGET
type CheckMGET struct {}
func (c *CheckMGET) CheckRequest (r *Request, s *Session) bool{
	return false
}
func (c *CheckMGET) CheckResponse (r *Request, s *Session, delay int64) bool{
	if len(r.Multi) < 2 {
		return true
	}
	if !r.Resp.IsArray() || len(r.Resp.Array) != len(r.Multi)-1 {
		return true
	}
	var hasBigValue bool
	var bigkeys = make([]KeyInfo, 0)

	for index, item := range r.Resp.Array {
		if int64(len(bigkeys)) >= GetMaxResultSetSize() {
			break
		}
		if item.IsBulkBytes() {
			if int64(len(item.Value)) >= GetMaxValueLength() {
				hasBigValue = true
				bigkeys = append(bigkeys, KeyInfo{
					Key: string(r.Multi[index+1].Value),
					DataSize: int64(len(item.Value))})
			}
		}
	}
	if hasBigValue {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = int64(len(r.Resp.Array))
		record.Details.KeyInfoList = bigkeys
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

// CMD: HGET
type CheckHGET struct {}
func (c *CheckHGET) CheckRequest(r *Request, s *Session) bool {
	return false
}
func (c *CheckHGET) CheckResponse(r *Request, s *Session, delay int64) bool {
	if len(r.Multi) < 3 {
		return true
	}
	key := r.Multi[1].Value
	var hasBigValue bool

	if r.Resp.IsBulkBytes() {
		hasBigValue = int64(len(r.Resp.Value)) >= GetMaxValueLength()
	}
	if hasBigValue {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
			Key: string(key),
			Batchsize: 1,
			DataSize: int64(len(r.Resp.Value)),
		})
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

// CMD: SDIFF
type CheckSDIFF struct {}
func (c *CheckSDIFF) CheckRequest(r *Request, s *Session) bool {
	return false
}
func (c *CheckSDIFF) CheckResponse(r *Request, s *Session, delay int64) bool {
	var isBatchTooLarge bool
	var batchsize int
	if len(r.Multi) < 2 { //至少包含1个集合
		return true
	}

	if r.Resp.IsArray() {
		batchsize = len(r.Resp.Array)
		isBatchTooLarge = int64(batchsize) >= GetMaxBatchSize()
	}
	if isBatchTooLarge {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		//写入key：key,key2,...,keyN
		for i:=1; i<len(r.Multi);i++ {
			if int64(len(record.Details.KeyInfoList)) >= GetMaxResultSetSize() {
				break
			}
			record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
				Key: string(r.Multi[i].Value)})
		}
		record.Details.ResponseBatchsize = int64(batchsize)
		record.Details.NumOfOperatedKeys = int64(len(r.Multi)-1)
		record.AbnormalType = TYPE_BIG_RESPONSE
		record.DelayUs = delay
		collectRecord(record)
	}
	return true

}

//*scan Request
func checkScanRequest(r *Request, s *Session) bool {
	//只监控该模式：xSCAN cursor [MATCH pattern] [COUNT count]
	if len(r.Multi) != 7 { //scan 不带数量则默认为10
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	count, err := strconv.Atoi(string(r.Multi[6].Value))
	if err != nil {
		return false
	}
	if int64(count) >= GetMaxBatchSize() {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_REQUEST
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
			Key: string(key),
			Batchsize: int64(count),
		})
		collectRecord(record)
		return true
	}
	return false
}
//CMD: HSCAN
type CheckHSCAN struct {}
func (c *CheckHSCAN) CheckRequest(r *Request, s *Session) bool {
	return checkScanRequest(r, s)
}
func (c *CheckHSCAN) CheckResponse(r *Request, s *Session, delay int64) bool {
	var datasizeTooLarge, tooMuchValues bool
	var fieldNum int64
	var bigkeys = make([]KeyInfo, 0)
	if len(r.Multi) < 2 {
		return true
	}
	key := r.Multi[1].Value

	if r.Resp.IsArray() {
		if len(r.Resp.Array) == 2 {
			respArray := r.Resp.Array[1]
			if respArray.IsArray() {
				arrLength := len(respArray.Array)
				if fieldNum = int64(arrLength/2); arrLength%2==0 {
					if fieldNum >= GetMaxBatchSize() {
						tooMuchValues = true
					}
					var keyinfo = KeyInfo{
						Key: string(key),
						Batchsize: fieldNum,
						MaxMemberSize: 0,
						DataSize: 0,
					}
					for i:=0; i<arrLength/2; i++ {
						valuesize := int64(len(respArray.Array[i*2+1].Value))
						keyinfo.DataSize += valuesize
						if keyinfo.MaxMemberSize <= valuesize {
							keyinfo.MaxMemberSize = valuesize
						}
						if valuesize >= GetMaxValueLength() {
							datasizeTooLarge = true
						}
					}
					if keyinfo.DataSize >= GetMaxValueLength() {
						datasizeTooLarge = true
					}
					bigkeys = append(bigkeys, keyinfo)
				}
			}
		}
	}
	if datasizeTooLarge || tooMuchValues {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = bigkeys
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}
//CMD: SSCAN
type CheckSSCAN struct {}
func (c *CheckSSCAN) CheckRequest(r *Request, s *Session) bool {
	return checkScanRequest(r, s)
}
func (c *CheckSSCAN) CheckResponse(r *Request, s *Session, delay int64) bool {
	var tooMuchValues bool
	if len(r.Multi) < 2 {
		return true
	}
	key := getHashKey(r.Multi, r.OpStr)

	keyinfo := KeyInfo{
		Key: string(key),
	}

	if r.Resp.IsArray() {
		if len(r.Resp.Array) == 2 {
			respArray := r.Resp.Array[1]
			if respArray.IsArray() {
				batchsize := int64(len(respArray.Array))
				keyinfo.MaxMemberSize = batchsize
				if batchsize >= GetMaxBatchSize() {
					tooMuchValues = true
				}
			}
		}
	}
	if tooMuchValues {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}
//CMD: ZSCAN
type CheckZSCAN struct {}
func (c *CheckZSCAN) CheckRequest(r *Request, s *Session) bool {
	return checkScanRequest(r, s)
}
func (c *CheckZSCAN) CheckResponse(r *Request, s *Session, delay int64) bool {
	if len(r.Multi) < 2 {
		return true
	}
	var tooMuchValues bool
	key := getHashKey(r.Multi, r.OpStr)
	keyinfo := KeyInfo{
		Key: string(key),
	}
	if r.Resp.IsArray() {
		if len(r.Resp.Array) == 2 {
			respArray := r.Resp.Array[1]
			if respArray.IsArray() {
				arrLength := len(respArray.Array)
				if batchsize := arrLength/2; arrLength%2==0 {
					keyinfo.Batchsize = int64(batchsize)
					if int64(batchsize) >= GetMaxBatchSize() {
						tooMuchValues = true
					}
				}
			}
		}
	}
	if tooMuchValues {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		record.Details.NumOfOperatedKeys = 1
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

//CMD: ZRANGE
func checkRangeRequest(r *Request, s *Session) bool {
	if len(r.Multi) < 4 { //少于4个值，非正常set命令
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	start, err := strconv.Atoi(string(r.Multi[2].Value))
	if err != nil {
		return false
	}
	end, err := strconv.Atoi(string(r.Multi[3].Value))
	if err != nil {
		return false
	}
	if start > end && end > 0 {
		return false
	}
	interval := int64(end-start+1)
	if interval >= GetMaxBatchSize() || end < 0 || interval < 0 {
		// interval < 0 属于线上的特殊场景，stop取了int64的上限值，使得interval的值越界为负，此处特殊处理
		var record = NewRecord(r, s.Conn.RemoteAddr())
		keyinfo := KeyInfo{
			Key: string(key),
		}
		if end < 0 {
			record.AbnormalType = TYPE_HIGH_RISK
		} else {
			record.AbnormalType = TYPE_BIG_REQUEST
		}
		if interval >= GetMaxBatchSize() {
			keyinfo.Batchsize = interval
		}
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		collectRecord(record)
		return true
	}
	return false
}
type CheckZRANGE struct {}
func (c *CheckZRANGE) CheckRequest(r *Request, s *Session) bool {
	return checkRangeRequest(r, s)
}
func (c *CheckZRANGE) CheckResponse(r *Request, s *Session, delay int64) bool {
	if len(r.Multi) < 4 { //少于4个值，非正常set命令
		return true
	}
	if !r.Resp.IsArray() {
		return true
	}
	var batchTooLarge bool
	key := getHashKey(r.Multi, r.OpStr)
	keyinfo := KeyInfo{
		Key: string(key),
	}

	if len(r.Multi) == 4 { //不带分数
		batchsize := int64(len(r.Resp.Array))
		keyinfo.Batchsize = batchsize
		batchTooLarge = batchsize >= GetMaxBatchSize()
	} else if len(r.Multi) == 5 {//带分数，withscores
		if keyword := strings.ToUpper(string(r.Multi[4].Value)); keyword != "WITHSCORES" {
			return true
		}//理论上withscores写错到不了这里，这里加上确保withscores拼写正确
		arrLength := len(r.Resp.Array)
		if arrLength % 2 != 0 {
			return true
		}
		batchsize := int64(arrLength/2)
		keyinfo.Batchsize = batchsize
		batchTooLarge = batchsize >= GetMaxBatchSize()
	}
	if batchTooLarge{
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_KEY
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, keyinfo)
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

//CMD: LRANGE
type CheckLRANGE struct {}
func (c *CheckLRANGE) CheckRequest(r *Request, s *Session) bool {
	return checkRangeRequest(r, s)
}
func (c *CheckLRANGE) CheckResponse(r *Request, s *Session, delay int64) bool {
	return false
}

//CMD: SINTER, SUNION
type CheckSETCOMPARE struct {}
func (c *CheckSETCOMPARE) CheckRequest(r *Request, s *Session) bool {
	return false
}
func (c *CheckSETCOMPARE) CheckResponse(r *Request, s *Session, delay int64) bool {
	if len(r.Multi) < 2 { //至少包含1个集合
		return true
	}
	var batchsize int
	var isBatchTooLarge bool
	if r.Resp.IsArray() {
		batchsize = len(r.Resp.Array)
		isBatchTooLarge = int64(batchsize) >= GetMaxBatchSize()
	}
	if isBatchTooLarge {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		//写入key：key,key2,...,keyN
		for i:=1; i<len(r.Multi); i++ {
			if int64(len(record.Details.KeyInfoList)) >= GetMaxResultSetSize() {
				break
			}
			record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
				Key: string(r.Multi[i].Value),
			})
		}
		record.Details.ResponseBatchsize = int64(batchsize)
		record.AbnormalType = TYPE_BIG_RESPONSE
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}

// CMD: SINTERSTORE, SUNIONSTORE
type CheckSETCOMPAREANDSTORE struct {}
func (c *CheckSETCOMPAREANDSTORE) CheckRequest(r *Request, s *Session) bool {
	return false
}
func (c *CheckSETCOMPAREANDSTORE) CheckResponse(r *Request, s *Session, delay int64) bool {
	if len(r.Multi) < 3 { //至少包含2个集合, 第一个为dest
		return true
	}
	var contentlength int = 0
	if r.Resp.IsInt() {
		res, err := strconv.Atoi(string(r.Resp.Value))
		if err != nil {
			return true
		}
		contentlength = res
	}
	if int64(contentlength) >= GetMaxBatchSize() {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		record.AbnormalType = TYPE_BIG_RESPONSE
		record.Details.ResponseBatchsize = int64(contentlength)
		for i:=1; i<len(r.Multi);i++ {
			if int64(len(record.Details.KeyInfoList)) >= GetMaxResultSetSize() {
				break
			}
			record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
				Key: string(r.Multi[i].Value),
			})
		}
		record.DelayUs = delay
		collectRecord(record)
	}
	return true
}


// CMD: SPOP
type CheckSPOP struct {}
func (c *CheckSPOP) CheckRequest(r *Request, s *Session) bool {
	if len(r.Multi) < 3 { //小于3说明不带数量
		return false
	}
	numToPop, err := strconv.Atoi(string(r.Multi[2].Value))
	if err != nil {
		return false
	}
	if int64(numToPop) >= GetMaxBatchSize() {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		key := getHashKey(r.Multi, r.OpStr)
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
			Key: string(key),
			Batchsize: int64(numToPop),
		})
		record.AbnormalType = TYPE_BIG_REQUEST
		collectRecord(record)
		return true
	}
	return false
}
func (c *CheckSPOP) CheckResponse(r *Request, s *Session, delay int64) bool {
	return false
}

// CMD: SRANDMEMBER
type CheckSRANDMEMBER struct {}
func (c *CheckSRANDMEMBER) CheckRequest(r *Request, s *Session) bool {
	if len(r.Multi) < 3 { //小于3说明不带数量
		return false
	}
	numToReturn, err := strconv.Atoi(string(r.Multi[2].Value))
	if err != nil {
		return false
	}
	if numToReturn < 0 {
		numToReturn = -numToReturn
	}
	if int64(numToReturn) >= GetMaxBatchSize() {
		var record = NewRecord(r, s.Conn.RemoteAddr())
		key := getHashKey(r.Multi, r.OpStr)
		record.Details.NumOfOperatedKeys = 1
		record.Details.KeyInfoList = append(record.Details.KeyInfoList, KeyInfo{
			Key: string(key),
			Batchsize: int64(numToReturn),
		})
		record.AbnormalType = TYPE_BIG_REQUEST
		collectRecord(record)
		return true
	}
	return false
}
func (c *CheckSRANDMEMBER) CheckResponse(r *Request, s *Session, delay int64) bool {
	return false
}













