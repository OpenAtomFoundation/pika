// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"container/list"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

const xSlowlogMaxLenMax = 10000000 //1000W	about max use 2G memory
const xSlowlogMaxLenDefault = 128000

// used by trylock
const mutexLocked = 1 << iota

// implement trylock for sync.Mutex
type Mutex struct {
	sync.Mutex
}

func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), 0, mutexLocked)
}

type XSlowlogEntry struct {
	id       int64
	time     int64
	duration int64
	cmd      string
}

type XSlowlog struct {
	Mutex
	loglist *list.List
	logid   atomic2.Int64
	maxlen  atomic2.Int64
}

var xSlowlog = &XSlowlog{}

func init() {
	xSlowlog.loglist = list.New()
	xSlowlog.logid.Swap(0)
	xSlowlog.maxlen.Swap(xSlowlogMaxLenDefault)
}

func XSlowlogSetMaxLen(maxlen int64) {
	defer xSlowlog.Unlock()
	xSlowlog.Lock()
	if maxlen < 0 {
		xSlowlog.maxlen.Swap(xSlowlogMaxLenDefault)
	} else if maxlen > xSlowlogMaxLenMax {
		xSlowlog.maxlen.Swap(xSlowlogMaxLenMax)
	} else {
		xSlowlog.maxlen.Swap(maxlen)
	}
}

/*
外层先通过XSlowlogGetCurId()接口获取logId，然后再调用XSlowlogPushFront()将日志插入，

	如果XSlowlogPushFront()获取锁失败将忽略该条记录，因此logId将不连续
*/
func XSlowlogGetCurId() int64 {
	return xSlowlog.logid.Incr()
}

func XSlowlogPushFront(e *XSlowlogEntry) {
	if e == nil || xSlowlog.maxlen.Int64() <= 0 {
		return
	}

	if xSlowlog.TryLock() {
		defer xSlowlog.Unlock()

		xSlowlog.loglist.PushFront(e)
		for int64(xSlowlog.loglist.Len()) > xSlowlog.maxlen.Int64() {
			xSlowlog.loglist.Remove(xSlowlog.loglist.Back())
		}
	} else {
		log.Warnf("cant get slowlog lock, logid: %d, cmd: %s", e.id, e.cmd)
	}
}

func XSlowlogLen() *redis.Resp {
	defer xSlowlog.Unlock()
	xSlowlog.Lock()
	return redis.NewString([]byte(strconv.Itoa(xSlowlog.loglist.Len())))
}

func XSlowlogReset() *redis.Resp {
	defer xSlowlog.Unlock()
	xSlowlog.Lock()

	xSlowlog.loglist.Init()
	xSlowlog.logid.Swap(0)

	return redis.NewString([]byte("OK"))
}

func slowLogToResp(e *XSlowlogEntry) *redis.Resp {
	if e == nil {
		return redis.NewArray(make([]*redis.Resp, 0))
	}
	return redis.NewArray([]*redis.Resp{
		redis.NewInt([]byte(strconv.FormatInt(e.id, 10))),
		redis.NewInt([]byte(strconv.FormatInt(e.time, 10))),
		redis.NewInt([]byte(strconv.FormatInt(e.duration, 10))),
		redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(e.cmd)),
		}),
	})
}

func XSlowlogGetByNum(num int64) *redis.Resp {
	defer xSlowlog.Unlock()
	xSlowlog.Lock()

	if num <= 0 {
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if num > int64(xSlowlog.loglist.Len()) {
		num = int64(xSlowlog.loglist.Len())
	}

	//array must init, or array is nil when there is no slowlog
	var array []*redis.Resp = make([]*redis.Resp, 0, num)

	var iter = xSlowlog.loglist.Front()
	var i int64
	for i = 0; i < num; i++ {
		if iter == nil || iter.Value == nil {
			break
		}

		if e, ok := iter.Value.(*XSlowlogEntry); ok {
			array = append(array, slowLogToResp(e))
		} else {
			log.Warnf("XSlowlogGet cont parse iter.Value[%v] to XSlowlogEntry.", iter.Value)
		}

		iter = iter.Next()
	}

	return redis.NewArray(array)
}

func XSlowlogGetById(id int64, num int64) *redis.Resp {
	defer xSlowlog.Unlock()
	xSlowlog.Lock()

	var lastId int64
	var lastNode = xSlowlog.loglist.Back()

	if lastNode == nil || lastNode.Value == nil {
		log.Warnf("XSlowlogGet lastNode or lastNode.Value == nil, lastNode: %v", lastNode)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if e, ok := lastNode.Value.(*XSlowlogEntry); ok {
		lastId = e.id
	} else {
		log.Warnf("XSlowlogGet cont parse lastNode.Value[%v] to XSlowlogEntry.", lastNode.Value)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if id < lastId || num <= 0 {
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if num > id-lastId+1 {
		num = id - lastId + 1
	}

	if num > int64(xSlowlog.loglist.Len()) {
		num = int64(xSlowlog.loglist.Len())
	}

	//array must init, or array is nil when there is no slowlog
	var array []*redis.Resp = make([]*redis.Resp, 0, num)
	var iter = xSlowlog.loglist.Front()

	//skip front element
	for ; iter != nil && iter.Value != nil; iter = iter.Next() {
		if e, ok := iter.Value.(*XSlowlogEntry); ok {
			if id >= e.id {
				break
			}
		} else {
			log.Warnf("XSlowlogGet cont parse iter.Value[%v] to XSlowlogEntry.", iter.Value)
		}
	}

	var i int64
	for i = 0; i < num; i++ {
		if iter == nil || iter.Value == nil {
			break
		}

		if e, ok := iter.Value.(*XSlowlogEntry); ok {
			array = append(array, slowLogToResp(e))
		} else {
			log.Warnf("XSlowlogGet cont parse iter.Value[%v] to XSlowlogEntry.", iter.Value)
		}

		iter = iter.Next()
	}

	return redis.NewArray(array)
}
