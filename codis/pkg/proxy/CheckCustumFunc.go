package proxy

import (
	"strconv"
	"strings"
)

// CMD: SET
type CheckSET struct {}
func (c *CheckSET) CheckRequest (r *Request) bool{
	if len(r.Multi) < 3 {
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	if r.Multi[2].IsBulkBytes() {
		if int64(len(r.Multi[2].Value)) >= GetMaxValueLength() {
			StoreKeyBlackList(string(key))
			return true
		}
	}
	return false
}
func (c *CheckSET) CheckResponse (r *Request) bool{
	return false
}

// CMD: MGET
type CheckMGET struct {}
func (c *CheckMGET) CheckRequest (r *Request) bool{
	return false
}
func (c *CheckMGET) CheckResponse (r *Request) bool{
	if len(r.Multi) < 2 {
		return true
	}
	if !r.Resp.IsArray() || len(r.Resp.Array) != len(r.Multi)-1 {
		return true
	}
	var hasBigValue bool
	var bigkeys = make([]string, 0)

	for index, item := range r.Resp.Array {
		if int64(len(bigkeys)) >= GetMaxResultSetSize() {
			break
		}
		if item.IsBulkBytes() {
			if int64(len(item.Value)) >= GetMaxValueLength() {
				hasBigValue = true
				bigkeys = append(bigkeys, string(r.Multi[index+1].Value))
			}
		}
	}
	if hasBigValue {
		StoreKeyBlackLists(strings.Join(bigkeys,","))
	}
	return true
}

// CMD: HGET
type CheckHGET struct {}
func (c *CheckHGET) CheckRequest(r *Request) bool {
	return false
}
func (c *CheckHGET) CheckResponse(r *Request) bool {
	if len(r.Multi) < 3 {
		return true
	}
	
	key := r.Multi[1].Value
	var hasBigValue bool
	if r.Resp.IsBulkBytes() {
		hasBigValue = int64(len(r.Resp.Value)) >= GetMaxValueLength()
	}
	if hasBigValue {
		StoreKeyBlackList(string(key))
	}
	return true
}

// CMD: SDIFF
type CheckSDIFF struct {}
func (c *CheckSDIFF) CheckRequest(r *Request) bool {
	return false
}
func (c *CheckSDIFF) CheckResponse(r *Request) bool {
	var isBatchTooLarge bool
	var batchsize int
	if len(r.Multi) < 2 { //contained at least one set
		return true
	}

	if r.Resp.IsArray() {
		batchsize = len(r.Resp.Array)
		isBatchTooLarge = int64(batchsize) >= GetMaxBatchSize()
	}
	if isBatchTooLarge {
		batch :=make([]string,0)
		for i:=1; i<len(r.Multi);i++ {
			if int64(len(batch)) >= GetMaxResultSetSize() {
				break
			}
			batch = append(batch,string(r.Multi[i].Value))
		}
	}
	return true

}

//*scan Request
func checkScanRequest(r *Request) bool {
	//Only this mode is detected：xSCAN cursor [MATCH pattern] [COUNT count]
	if len(r.Multi) != 7 { //scan default is 10
		return false
	}
	count, err := strconv.Atoi(string(r.Multi[6].Value))
	if err != nil {
		return false
	}
	if int64(count) >= GetMaxBatchSize() {
		return true
	}
	return false
}
//CMD: HSCAN
type CheckHSCAN struct {}
func (c *CheckHSCAN) CheckRequest(r *Request) bool {
	return checkScanRequest(r)
}
func (c *CheckHSCAN) CheckResponse(r *Request) bool {
	var datasizeTooLarge, tooMuchValues bool
	var fieldNum int64
	var bigkeys = make([]string, 0)
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
					var (
						keyinfo = string(key)
						MaxMemberSize int64 = 0
						DataSize int64 = 0
					)
					for i:=0; i<arrLength/2; i++ {
						valuesize := int64(len(respArray.Array[i*2+1].Value))
						DataSize += valuesize
						if MaxMemberSize <= valuesize {
							MaxMemberSize = valuesize
						}
						if valuesize >= GetMaxValueLength() {
							datasizeTooLarge = true
						}
					}
					if DataSize >= GetMaxValueLength() {
						datasizeTooLarge = true
					}
					bigkeys = append(bigkeys, keyinfo)
				}
			}
		}
	}
	if datasizeTooLarge || tooMuchValues {
		StoreKeyBlackLists(strings.Join(bigkeys,","))
	}
	return true
}
//CMD: SSCAN
type CheckSSCAN struct {}
func (c *CheckSSCAN) CheckRequest(r *Request) bool {
	return checkScanRequest(r)
}
func (c *CheckSSCAN) CheckResponse(r *Request) bool {
	var tooMuchValues bool
	if len(r.Multi) < 2 {
		return true
	}
	key := getHashKey(r.Multi, r.OpStr)
	if r.Resp.IsArray() {
		if len(r.Resp.Array) == 2 {
			respArray := r.Resp.Array[1]
			if respArray.IsArray() {
				batchsize := int64(len(respArray.Array))
				if batchsize >= GetMaxBatchSize() {
					tooMuchValues = true
				}
			}
		}
	}
	if tooMuchValues {
		StoreKeyBlackList(string(key))
	}
	return true
}
//CMD: ZSCAN
type CheckZSCAN struct {}
func (c *CheckZSCAN) CheckRequest(r *Request) bool {
	return checkScanRequest(r)
}
func (c *CheckZSCAN) CheckResponse(r *Request) bool {
	if len(r.Multi) < 2 {
		return true
	}
	var tooMuchValues bool
	key := getHashKey(r.Multi, r.OpStr)
	if r.Resp.IsArray() {
		if len(r.Resp.Array) == 2 {
			respArray := r.Resp.Array[1]
			if respArray.IsArray() {
				arrLength := len(respArray.Array)
				if batchsize := arrLength/2; arrLength%2==0 {
					if int64(batchsize) >= GetMaxBatchSize() {
						tooMuchValues = true
					}
				}
			}
		}
	}
	if tooMuchValues {
		StoreKeyBlackList(string(key))
	}
	return true
}

//CMD: ZRANGE
func checkRangeRequest(r *Request) bool {
	if len(r.Multi) < 4 {
		return false
	}
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
		// when stop take the upper limit of int64，makes the value of interval cross the boundary to negative
		if end < 0 {
			return true
		} else if interval >= GetMaxBatchSize(){
			return true
	}
		return true
	}
	return false
}
type CheckZRANGE struct {}
func (c *CheckZRANGE) CheckRequest(r *Request) bool {
	return checkRangeRequest(r)
}
func (c *CheckZRANGE) CheckResponse(r *Request) bool {
	if len(r.Multi) < 4 {
		return true
	}
	if !r.Resp.IsArray() {
		return true
	}
	var batchTooLarge bool
	if len(r.Multi) == 4 { //without withscores
		batchsize := int64(len(r.Resp.Array))
		batchTooLarge = batchsize >= GetMaxBatchSize()
	} else if len(r.Multi) == 5 {//withscores
		if keyword := strings.ToUpper(string(r.Multi[4].Value)); keyword != "WITHSCORES" {
			return true
		}
		arrLength := len(r.Resp.Array)
		if arrLength % 2 != 0 {
			return true
		}
		batchsize := int64(arrLength/2)
		batchTooLarge = batchsize >= GetMaxBatchSize()
	}
	if batchTooLarge{
		return true
	}
	return false
}

//CMD: LRANGE
type CheckLRANGE struct {}
func (c *CheckLRANGE) CheckRequest(r *Request) bool {
	return checkRangeRequest(r)
}
func (c *CheckLRANGE) CheckResponse(r *Request) bool {
	return false
}
// CMD: SPOP
type CheckSPOP struct {}
func (c *CheckSPOP) CheckRequest(r *Request) bool {
	if len(r.Multi) < 3 { //without count
		return false
	}
	numToPop, err := strconv.Atoi(string(r.Multi[2].Value))
	if err != nil {
		return false
	}
	if int64(numToPop) >= GetMaxBatchSize() {
		return true
	}
	return false
}
func (c *CheckSPOP) CheckResponse(r *Request) bool {
	return false
}

// CMD: SRANDMEMBER
type CheckSRANDMEMBER struct {}
func (c *CheckSRANDMEMBER) CheckRequest(r *Request) bool {
	if len(r.Multi) < 3 { //without count
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
		return true
	}
	return false
}
func (c *CheckSRANDMEMBER) CheckResponse(r *Request) bool {
	return false
}













