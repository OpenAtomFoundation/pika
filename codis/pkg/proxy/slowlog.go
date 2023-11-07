package proxy

import (
	"container/list"
	"strconv"
	"sync"

	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
)

const (
	PIKA_SLOWLOG_LENGTH_DEFAULT = 128000
	PIKA_SLOWLOG_LENGTH_MAX     = 10000000
)

type Mutex struct {
	sync.Mutex
}

type SlowLogEntry struct {
	id       int64
	time     int64
	duration int64
	cmd      string
}

type SlowLog struct {
	Mutex
	logList *list.List
	logId   atomic2.Int64
	maxLen  atomic2.Int64
}

var PSlowLog = &SlowLog{}

func init() {
	PSlowLog.logList = list.New()
	PSlowLog.logId.Swap(0)
	PSlowLog.maxLen.Swap(PIKA_SLOWLOG_LENGTH_DEFAULT)
}

func SlowLogSetMaxLen(len int64) {
	if len < 0 {
		PSlowLog.maxLen.Swap(PIKA_SLOWLOG_LENGTH_DEFAULT)
	} else if len > PIKA_SLOWLOG_LENGTH_MAX {
		PSlowLog.maxLen.Swap(PIKA_SLOWLOG_LENGTH_MAX)
	} else {
		PSlowLog.maxLen.Swap(len)
	}
}

func SlowLogGetCurLogId() int64 {
	return PSlowLog.logId.Incr()
}

func SlowLogPush(entry *SlowLogEntry) {
	if entry == nil || PSlowLog.maxLen <= 0 {
		return
	}
	if PSlowLog.TryLock() {
		defer PSlowLog.Unlock()
		PSlowLog.logList.PushFront(entry) // push a ptr
		for int64(PSlowLog.logList.Len()) > PSlowLog.maxLen.Int64() {
			PSlowLog.logList.Remove(PSlowLog.logList.Back())
		}
	} else {
		log.Warnf("cant get slowlog lock, logid: %d, cmd: %s", entry.id, entry.cmd)
	}
}

func SlowLogGetLen() *redis.Resp {
	PSlowLog.Lock()
	defer PSlowLog.Unlock()
	return redis.NewString([]byte(strconv.Itoa(PSlowLog.logList.Len())))
}

func SlowLogReset() *redis.Resp {
	PSlowLog.Lock()
	defer PSlowLog.Unlock()
	PSlowLog.logId.Swap(0)
	PSlowLog.logList.Init()
	return redis.NewString([]byte("OK"))
}

func SlowLogToResp(entry *SlowLogEntry) *redis.Resp {
	if entry == nil {
		return redis.NewArray(make([]*redis.Resp, 0))
	}
	return redis.NewArray([]*redis.Resp{
		redis.NewInt([]byte(strconv.FormatInt(entry.id, 10))),
		redis.NewInt([]byte(strconv.FormatInt(entry.time, 10))),
		redis.NewInt([]byte(strconv.FormatInt(entry.duration, 10))),
		redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(entry.cmd)),
		}),
	})
}

func SlowLogGetByNum(num int64) *redis.Resp {
	PSlowLog.Lock()
	defer PSlowLog.Unlock()
	if num <= 0 {
		return redis.NewArray(make([]*redis.Resp, 0))
	} else if num > int64(PSlowLog.logList.Len()) {
		num = int64(PSlowLog.logList.Len())
	}
	var res = make([]*redis.Resp, 0, num)
	var iter = PSlowLog.logList.Front() //  从最新的数据开始
	for i := int64(0); i < num; i++ {
		if iter == nil || iter.Value == nil {
			break
		}
		if entry, ok := iter.Value.(*SlowLogEntry); ok {
			res = append(res, SlowLogToResp(entry))
		} else {
			log.Warnf("slowLogGet cont parse iter.Value[%v] to slowLogEntry.", iter.Value)
		}
		iter = iter.Next()
	}
	return redis.NewArray(res)
}

func SlowLogGetByIdAndNUm(id, num int64) *redis.Resp {
	PSlowLog.Lock()
	defer PSlowLog.Unlock()

	var smallestID int64
	var oldestNode = PSlowLog.logList.Back()
	if oldestNode == nil || oldestNode.Value == nil {
		log.Warnf("slowlogGet oldestNode or oldestNode.Value == nil, oldestNode: %v", oldestNode)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if entry, ok := oldestNode.Value.(*SlowLogEntry); ok {
		smallestID = entry.id
	} else {
		log.Warnf("slowlogGet cont parse oldestNode.Value[%v] to slowlogEntry.", oldestNode.Value)
	}
	if id < smallestID || num < 0 {
		return redis.NewArray(make([]*redis.Resp, 0))
	}
	if num > id-smallestID+1 {
		num = id - smallestID + 1
	}

	if num > int64(PSlowLog.logList.Len()) {
		num = int64(PSlowLog.logList.Len())
	}

	var res = make([]*redis.Resp, num)
	var iter = PSlowLog.logList.Front()

	for ; iter != nil && iter.Value != nil; iter = iter.Next() {
		if entry, ok := iter.Value.(*SlowLogEntry); ok {
			if id >= entry.id {
				break
			}
		} else {
			log.Warnf("slowlogGet cont parse iter.Value[%v] to slowlogEntry.", iter.Value)
		}
	}

	for i := int64(0); i < num; i++ {
		if iter == nil || iter.Value == nil {
			break
		}
		if entry, ok := iter.Value.(*SlowLogEntry); ok {
			res = append(res, SlowLogToResp(entry))
		} else {
			log.Warnf("slowlogGet cont parse iter.Value[%v] to slowlogEntry.", iter.Value)
		}
		iter = iter.Next()
	}
	return redis.NewArray(res)
}
