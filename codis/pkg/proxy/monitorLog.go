// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"container/list"
	"encoding/json"
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

const mlogMaxLenMax = 100000 //10W	about max use 8G memory
const mlogMaxLenDefault = 10000

// used by trylock
const mlogMutexLocked = 1 << iota

const MONITOR_GET_BIG_KEY = 1
const MONITOR_GET_RISK_CMD = 2
const MONITOR_GET_ALL = 3

// implement trylock for sync.Mutex
type MlogMutex struct {
	sync.Mutex
}

func (m *MlogMutex) TryLock() bool {
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), 0, mlogMutexLocked)
}

type MlogEntry struct {
	id  int64   //记录ID
	log *Record //记录内容
}

type MonitorLog struct {
	MlogMutex
	loglist *list.List
	logid   atomic2.Int64
	maxlen  atomic2.Int64
}

var mlog = &MonitorLog{}

func init() {
	mlog.loglist = list.New()
	mlog.logid.Swap(0)
	mlog.maxlen.Swap(mlogMaxLenDefault)
}

func MonitorLogSetMaxLen(maxlen int64) {
	defer mlog.Unlock()
	mlog.Lock()
	if maxlen < 0 {
		mlog.maxlen.Swap(mlogMaxLenDefault)
	} else if maxlen > mlogMaxLenMax {
		mlog.maxlen.Swap(mlogMaxLenMax)
	} else {
		mlog.maxlen.Swap(maxlen)
	}
}

/*
外层先通过MlogListGetCurId()接口获取logId，然后再调用MlogListPushFront()将日志插入，

	如果MlogListPushFront()获取锁失败将忽略该条记录，因此logId将不连续
*/
func MonitorLogGetCurId() int64 {
	return mlog.logid.Incr()
}

func MonitorLogPushBack(e *MlogEntry) {
	if e == nil || mlog.maxlen.Int64() <= 0 {
		return
	}
	if mlog.TryLock() {
		defer mlog.Unlock()

		mlog.loglist.PushBack(e)
		for int64(mlog.loglist.Len()) > mlog.maxlen.Int64() {
			mlog.loglist.Remove(mlog.loglist.Front())
		}
	} else {
		log.Warnf("cant get monitorlog lock, logid: %d, log: %+v", e.id, e.log)
	}
}

func MonitorLogLen() *redis.Resp {
	defer mlog.Unlock()
	mlog.Lock()
	return redis.NewString([]byte(strconv.Itoa(mlog.loglist.Len())))
}

func MonitorLogReset(force bool) *redis.Resp {
	defer mlog.Unlock()
	mlog.Lock()

	mlog.loglist.Init()
	if force {
		mlog.logid.Swap(0)
	}

	return redis.NewString([]byte("OK"))
}

func monitorLogToResp(e *MlogEntry) *redis.Resp {
	if e == nil {
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	//将格式化的数据转换成字符串
	byteLog, err := json.Marshal(e.log)
	if err != nil {
		log.Warnf("fail to format record into json, err: ", err)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	return redis.NewArray([]*redis.Resp{
		redis.NewInt([]byte(strconv.FormatInt(e.id, 10))),
		redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(byteLog)),
		}),
	})
}

func MonitorLogGetByNum(num int64, recordType int64) *redis.Resp {
	//从尾部（最新的记录）开始获取num条记录
	defer mlog.Unlock()
	mlog.Lock()

	if num <= 0 {
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if num > int64(mlog.loglist.Len()) {
		num = int64(mlog.loglist.Len())
	}

	//array must init, or array is nil when there is no monitorlog
	var array []*redis.Resp = make([]*redis.Resp, 0, num)

	var iter = mlog.loglist.Back()
	var i int64
	for i = 0; i < num; {
		if iter == nil || iter.Value == nil {
			break
		}

		if e, ok := iter.Value.(*MlogEntry); ok && e != nil && e.log != nil {
			switch recordType {
			case MONITOR_GET_BIG_KEY:
				// 只返回大key、大value监控
				if e.log.AbnormalType != TYPE_HIGH_RISK {
					array = append(array, monitorLogToResp(e))
					i++
				}
			case MONITOR_GET_RISK_CMD:
				// 只返回高危命令
				if e.log.AbnormalType == TYPE_HIGH_RISK {
					array = append(array, monitorLogToResp(e))
					i++
				}
			default:
				// 返回所有类型
				array = append(array, monitorLogToResp(e))
				i++
			}
		} else {
			log.Warnf("MonitorlogGet cant parse iter.Value[%+v] to MonitorlogEntry.", iter.Value)
		}

		iter = iter.Prev()
	}

	return redis.NewArray(array)
}

func MonitorLogGetById(id int64, num int64, recordType int64) *redis.Resp {
	defer mlog.Unlock()
	mlog.Lock()

	var lastId int64 //尾部的ID理论是最大的
	var lastNode = mlog.loglist.Back()

	if lastNode == nil || lastNode.Value == nil {
		log.Warnf("MonitorlogGet lastNode or lastNode.Value == nil, lastNode: %v", lastNode)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if e, ok := lastNode.Value.(*MlogEntry); ok {
		lastId = e.id
	} else {
		log.Warnf("MonitorlogGet cant parse lastNode.Value[%v] to MlogEntry.", lastNode.Value)
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if id > lastId || num <= 0 { //如果送入的id比尾部的id还大，说明不存在，所以忽略
		return redis.NewArray(make([]*redis.Resp, 0))
	}

	if num > lastId-id+1 { //如果索取的数量比（尾部-游标）的间隔还大，则只取（尾部-游标）之间的元素
		num = lastId - id + 1
	}

	if num > int64(mlog.loglist.Len()) {
		num = int64(mlog.loglist.Len())
	}

	//array must init, or array is nil when there is no monitorlog
	var array []*redis.Resp = make([]*redis.Resp, 0, num)
	var iter = mlog.loglist.Back()

	//skip front element
	for ; iter != nil && iter.Value != nil; iter = iter.Prev() {
		if e, ok := iter.Value.(*MlogEntry); ok {
			if id > e.id { //找到这个游标位置的元素，可能不是那个，有可能该位置因为log_id断层，log_id比给定的要大
				iter = iter.Next()
				break
			} else if id == e.id {
				break
			}
		} else {
			log.Warnf("monitorlogGet cont parse iter.Value[%v] to monitorlogEntry.", iter.Value)
		}
	}

	var i int64
	for i = 0; i < num; i++ {
		if iter == nil || iter.Value == nil {
			break
		}

		if e, ok := iter.Value.(*MlogEntry); ok && e != nil && e.log != nil {
			switch recordType {
			case MONITOR_GET_BIG_KEY:
				// 只返回大key、大value监控
				if e.log.AbnormalType != TYPE_HIGH_RISK {
					array = append(array, monitorLogToResp(e))
				}
			case MONITOR_GET_RISK_CMD:
				// 只返回高危命令
				if e.log.AbnormalType == TYPE_HIGH_RISK {
					array = append(array, monitorLogToResp(e))
				}
			default:
				array = append(array, monitorLogToResp(e))
			}
		} else {
			log.Warnf("monitorlogGet cont parse iter.Value[%v] to monitorlogEntry.", iter.Value)
		}

		iter = iter.Next()
	}

	return redis.NewArray(array)
}
