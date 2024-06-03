// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
)

const (
	TPFirstGrade            = 5 // 5ms - 200ms
	TPFirstGradeSize        = 40
	TPSecondGrade           = 25 // 225ms - 700ms
	TPSecondGradeSize       = 20
	TPThirdGrade            = 250 // 950ms - 3200ms
	TPThirdGradeSize        = 10
	TPMaxNum                = TPFirstGradeSize + TPSecondGradeSize + TPThirdGradeSize
	ClearSlowFlagPeriodRate = 3 // The cleanup cycle for slow commands is three times the duration of the statistics cycle.
	IntervalNum             = 5
	DelayKindNum            = 8
)

var (
	// Unit: s
	IntervalMark    = [IntervalNum]int64{1, 10, 60, 600, 3600}
	LastRefreshTime = [IntervalNum]time.Time{time.Now()}

	// Unit: ms
	DelayNumMark = [DelayKindNum]int64{50, 100, 200, 300, 500, 1000, 2000, 3000}
)

type delayInfo struct {
	interval int64
	calls    atomic2.Int64
	nsecs    atomic2.Int64
	nsecsmax atomic2.Int64
	avg      int64
	qps      atomic2.Int64

	tp     [TPMaxNum]atomic2.Int64
	tp90   int64
	tp99   int64
	tp999  int64
	tp9999 int64
	tp100  int64

	delayCount [DelayKindNum]atomic2.Int64
	delay50ms  int64
	delay100ms int64
	delay200ms int64
	delay300ms int64
	delay500ms int64
	delay1s    int64
	delay2s    int64
	delay3s    int64
}

type opStats struct {
	opstr             string
	calls             atomic2.Int64
	nsecs             atomic2.Int64
	fails             atomic2.Int64
	lastSetSlowTime   int64
	lastClearSlowTime int64

	delayInfo [IntervalNum]*delayInfo

	redis struct {
		errors atomic2.Int64
	}
	maxDelay atomic2.Int64
}

type OpStats struct {
	OpStr        string `json:"opstr"`
	Interval     int64  `json:"interval"`
	TotalCalls   int64  `json:"total_calls"`
	TotalUsecs   int64  `json:"total_usecs"`
	UsecsPercall int64  `json:"usecs_percall"`

	Calls        int64 `json:"calls"`
	Usecs        int64 `json:"usecs"`
	Fails        int64 `json:"fails"`
	RedisErrType int64 `json:"redis_errtype"`
	MaxDelay     int64 `json:"max_delay"`
	QPS          int64 `json:"qps"`
	AVG          int64 `json:"avg"`
	TP90         int64 `json:"tp90"`
	TP99         int64 `json:"tp99"`
	TP999        int64 `json:"tp999"`
	TP9999       int64 `json:"tp9999"`
	TP100        int64 `json:"tp100"`

	Delay50ms  int64 `json:"delay50ms"`
	Delay100ms int64 `json:"delay100ms"`
	Delay200ms int64 `json:"delay200ms"`
	Delay300ms int64 `json:"delay300ms"`
	Delay500ms int64 `json:"delay500ms"`
	Delay1s    int64 `json:"delay1s"`
	Delay2s    int64 `json:"delay2s"`
	Delay3s    int64 `json:"delay3s"`
}

var (
	SlowCmdCount  atomic2.Int64 // Cumulative count of slow log
	RefreshPeriod atomic2.Int64
)

var cmdstats struct {
	sync.RWMutex //Lock only for opmap.
	opmap        map[string]*opStats

	total atomic2.Int64
	fails atomic2.Int64
	redis struct {
		errors atomic2.Int64
	}

	qps             atomic2.Int64
	tpdelay         [TPMaxNum]int64 //us
	refreshPeriod   atomic2.Int64
	logSlowerThan   atomic2.Int64
	autoSetSlowFlag atomic2.Bool
}

func (s *opStats) OpStats() *OpStats {
	o := &OpStats{
		OpStr:    s.opstr,
		Calls:    s.calls.Int64(),
		Usecs:    s.nsecs.Int64() / 1e3,
		Fails:    s.fails.Int64(),
		MaxDelay: s.maxDelay.Int64(),
	}
	if o.Calls != 0 {
		o.UsecsPercall = o.Usecs / o.Calls
	}
	o.RedisErrType = s.redis.errors.Int64()
	return o
}

func incrOpStats(e *opStats) {
	s := getOpStats(e.opstr, true)
	s.calls.Add(e.calls.Swap(0))
	s.nsecs.Add(e.nsecs.Swap(0))
	if n := e.fails.Swap(0); n != 0 {
		s.fails.Add(n)
		cmdstats.fails.Add(n)
	}
	if n := e.redis.errors.Swap(0); n != 0 {
		s.redis.errors.Add(n)
		cmdstats.redis.errors.Add(n)
	}

	/**
	Each session refreshes its own saved metrics, and there is a race condition at this time.
	Use the CAS method to update.
	*/
	for {
		oldValue := s.maxDelay
		if e.maxDelay > oldValue {
			if s.maxDelay.CompareAndSwap(oldValue.Int64(), e.maxDelay.Int64()) {
				e.maxDelay.Set(0)
				break
			}
		} else {
			break
		}
	}
}

func init() {
	cmdstats.opmap = make(map[string]*opStats, 128)
	cmdstats.refreshPeriod.Set(int64(time.Second))

	//init tp delay array
	for i := 0; i < TPMaxNum; i++ {
		if i < TPFirstGradeSize {
			cmdstats.tpdelay[i] = int64(i+1) * TPFirstGrade
		} else if i < TPFirstGradeSize+TPSecondGradeSize {
			cmdstats.tpdelay[i] = TPFirstGradeSize*TPFirstGrade + int64(i-TPFirstGradeSize+1)*TPSecondGrade
		} else {
			cmdstats.tpdelay[i] = TPFirstGradeSize*TPFirstGrade + TPSecondGradeSize*TPSecondGrade + int64(i-TPFirstGradeSize-TPSecondGradeSize+1)*TPThirdGrade
		}
	}

	// init LastRefreshTime array
	for i := 0; i < IntervalNum; i++ {
		LastRefreshTime[i] = time.Now()
	}

	go func() {
		for {
			start := time.Now()
			total := cmdstats.total.Int64()
			time.Sleep(time.Second)
			delta := cmdstats.total.Int64() - total
			normalized := math.Max(0, float64(delta)) * float64(time.Second) / float64(time.Since(start))
			cmdstats.qps.Set(int64(normalized + 0.5))

			cmdstats.RLock()
			for i := 0; i < IntervalNum; i++ {
				/*if int64(float64(time.Since(LastRefreshTime[i]))/float64(time.Second)) < IntervalMark[i] {
					continue
				}*/
				for _, v := range cmdstats.opmap {
					v.RefreshOpStats(i)
				}
				LastRefreshTime[i] = time.Now()
			}
			cmdstats.RUnlock()
		}
	}()
}

func (s *delayInfo) refreshTpInfo(cmd string) {
	s.refresh4TpInfo(cmd)
	s.tp100 = s.nsecsmax.Int64() / 1e6
	if calls := s.calls.Int64(); calls != 0 {
		s.avg = s.nsecs.Int64() / 1e6 / calls
	} else {
		s.avg = 0
	}
}

func (s *delayInfo) refresh4TpInfo(cmd string) {
	persents1 := 0.9
	persents2 := 0.99
	persents3 := 0.999
	persents4 := 0.9999

	if s.calls.Int64() == 0 {
		s.tp90 = 0
		s.tp99 = 0
		s.tp999 = 0
		s.tp9999 = 0
		return
	}

	tpnum1 := int64(float64(s.calls.Int64()) * persents1)
	tpnum2 := int64(float64(s.calls.Int64()) * persents2)
	tpnum3 := int64(float64(s.calls.Int64()) * persents3)
	tpnum4 := int64(float64(s.calls.Int64()) * persents4)

	var index1, index2, index3, index4 int
	var count int64
	var i int

	for i = 0; i < len(s.tp); i++ {
		count += s.tp[i].Int64()
		if count >= tpnum1 || i == len(s.tp)-1 {
			index1 = i
			break
		}
	}

	if count >= tpnum2 || i == len(s.tp)-1 {
		index2 = i
	} else {
		for i = i + 1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum2 || i == len(s.tp)-1 {
				index2 = i
				break
			}
		}
	}

	if count >= tpnum3 || i == len(s.tp)-1 {
		index3 = i
	} else {
		for i = i + 1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum3 || i == len(s.tp)-1 {
				index3 = i
				break
			}
		}
	}

	if count >= tpnum4 || i == len(s.tp)-1 {
		index4 = i
	} else {
		for i = i + 1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum4 || i == len(s.tp)-1 {
				index4 = i
				break
			}
		}
	}

	// If an anomaly occurs in the statistics, print a log line.
	if i == len(s.tp)-1 && s.tp[i].Int64() <= 0 {
		log.Warnf("refreshTpInfo err: cmd-[%s] tpinfo is unavailable", cmd)
	}

	if index1 >= 0 && index2 >= index1 && index3 >= index2 && index4 >= index3 && index4 < TPMaxNum {
		s.tp90 = cmdstats.tpdelay[index1]
		s.tp99 = cmdstats.tpdelay[index2]
		s.tp999 = cmdstats.tpdelay[index3]
		s.tp9999 = cmdstats.tpdelay[index4]
		return
	}

	log.Warnf("refreshTpInfo err: cmd-[%s] reset exception tpinf", cmd)
	s.tp90 = -1
	s.tp99 = -1
	s.tp999 = -1
	s.tp9999 = -1
	return
}

func (s *delayInfo) resetTpInfo() {
	s.calls.Set(0)
	s.nsecs.Set(0)
	s.nsecsmax.Set(0)
	s.tp = [TPMaxNum]atomic2.Int64{0}
}

func (s *delayInfo) refreshDelayInfo() {
	s.delay50ms = s.delayCount[0].Int64()
	s.delay100ms = s.delayCount[1].Int64()
	s.delay200ms = s.delayCount[2].Int64()
	s.delay300ms = s.delayCount[3].Int64()
	s.delay500ms = s.delayCount[4].Int64()
	s.delay1s = s.delayCount[5].Int64()
	s.delay2s = s.delayCount[6].Int64()
	s.delay3s = s.delayCount[7].Int64()
}

func (s *delayInfo) resetDelayInfo() {
	s.delayCount = [DelayKindNum]atomic2.Int64{0}
}

// The unit of duration in IncrTP() is nanoseconds (ns).
func (s *opStats) incrTP(duration int64) {
	var index int64 = -1
	var duration_ms int64 = duration / 1e6
	if duration_ms <= 0 {
		//s.tp[0].Incr()
		index = 0
	} else if duration_ms <= TPFirstGrade*TPFirstGradeSize {
		index = (duration_ms+TPFirstGrade-1)/TPFirstGrade - 1
		//s.tp[index].Incr()
	} else if duration_ms <= TPFirstGrade*TPFirstGradeSize+TPSecondGrade*TPSecondGradeSize {
		index = (duration_ms-TPFirstGrade*TPFirstGradeSize+TPSecondGrade-1)/TPSecondGrade + TPFirstGradeSize - 1
		//s.tp[index].Incr()
	} else if duration_ms <= TPFirstGrade*TPFirstGradeSize+TPSecondGrade*TPSecondGradeSize+TPThirdGrade*TPThirdGradeSize {
		index = (duration_ms-TPFirstGrade*TPFirstGradeSize-TPSecondGrade*TPSecondGradeSize+TPThirdGrade-1)/TPThirdGrade + TPFirstGradeSize + TPSecondGradeSize - 1
		//s.tp[index].Incr()
	} else {
		index = TPMaxNum - 1
		//s.tp[TPMaxNum-1].Incr()
	}

	if index < 0 {
		return
	}

	for i := 0; i < IntervalNum; i++ {
		s.delayInfo[i].calls.Incr()
		s.delayInfo[i].nsecs.Add(duration)
		lastMax := s.delayInfo[i].nsecsmax.Int64()
		// Set the maximum error of the max value to 5ms to prevent multiple threads from updating simultaneously.
		if duration >= lastMax+5*1e6 {
			for {
				ok := s.delayInfo[i].nsecsmax.CompareAndSwap(lastMax, duration)
				if ok {
					break
				} else {
					lastMax = s.delayInfo[i].nsecsmax.Int64()
					if duration < lastMax+5*1e6 {
						//log.Warnf("CompareAndSwap return false and break, newMax is [%d] lastMax is [%d] now time is [%v], ",duration, lastMax, time.Now())
						break

					}
					log.Warnf("CompareAndSwap return false and try again, newMax is [%d ns] lastMax is [%d ns]", duration, lastMax)
				}
			}
		}
		s.delayInfo[i].tp[index].Incr()
	}
}

func (s *opStats) RefreshOpStats(index int) {
	if index < 0 || index >= IntervalNum {
		return
	}
	normalized := math.Max(0, float64(s.delayInfo[index].calls.Int64())) / float64(time.Since(LastRefreshTime[index])) * float64(time.Second)
	s.delayInfo[index].qps.Set(int64(normalized + 0.5))
	s.delayInfo[index].refreshTpInfo(s.opstr)
	s.delayInfo[index].resetTpInfo()

	// Count the number of timed-out commands.
	s.delayInfo[index].refreshDelayInfo()
	s.delayInfo[index].resetDelayInfo()
}

// The unit of duration is milliseconds (ms).
func (s *opStats) incrDelayNum(duration int64) {
	for i, v := range DelayNumMark {
		if duration >= v {
			for j, _ := range IntervalMark {
				s.delayInfo[j].delayCount[i].Incr()
			}
		} else {
			break
		}
	}
}

func (s *opStats) GetOpStatsByInterval(interval int64) *OpStats {
	var index int64 = -1
	var i int64
	for i = 0; i < IntervalNum; i++ {
		if interval == IntervalMark[i] {
			index = i
		}
	}
	if index < 0 {
		index = 0
	}

	o := &OpStats{
		OpStr:      s.opstr,
		Interval:   s.delayInfo[index].interval,
		Calls:      s.calls.Int64(),
		Usecs:      s.nsecs.Int64() / 1e3,
		Fails:      s.fails.Int64(),
		TotalCalls: s.delayInfo[index].calls.Int64(),
		TotalUsecs: s.delayInfo[index].nsecs.Int64() / 1e3,
		QPS:        s.delayInfo[index].qps.Int64(),
		AVG:        s.delayInfo[index].avg,
		TP90:       s.delayInfo[index].tp90,
		TP99:       s.delayInfo[index].tp99,
		TP999:      s.delayInfo[index].tp999,
		TP9999:     s.delayInfo[index].tp9999,
		TP100:      s.delayInfo[index].tp100,
		Delay50ms:  s.delayInfo[index].delay50ms,
		Delay100ms: s.delayInfo[index].delay100ms,
		Delay200ms: s.delayInfo[index].delay200ms,
		Delay300ms: s.delayInfo[index].delay300ms,
		Delay500ms: s.delayInfo[index].delay500ms,
		Delay1s:    s.delayInfo[index].delay1s,
		Delay2s:    s.delayInfo[index].delay2s,
		Delay3s:    s.delayInfo[index].delay3s,
	}

	if o.Calls != 0 {
		o.UsecsPercall = o.Usecs / o.Calls
	}
	o.RedisErrType = s.redis.errors.Int64()

	return o
}

func (s *opStats) incrOpStats(responseTime int64, t redis.RespType) {
	s.calls.Incr()
	s.nsecs.Add(responseTime)
	switch t {
	case redis.TypeError:
		s.redis.errors.Incr()
	}

	// Collect TP (transaction processing) data.
	s.incrTP(responseTime)
	// Count the number of timeout commands.
	s.incrDelayNum(responseTime / 1e6)
}

func StatsSetRefreshPeriod(d time.Duration) {
	if d >= 0 {
		cmdstats.refreshPeriod.Set(int64(d))
	}
}

func OpTotal() int64 {
	return cmdstats.total.Int64()
}

func OpFails() int64 {
	return cmdstats.fails.Int64()
}

func OpRedisErrors() int64 {
	return cmdstats.redis.errors.Int64()
}

func OpQPS() int64 {
	return cmdstats.qps.Int64()
}

func getOpStats(opstr string, create bool) *opStats {
	cmdstats.RLock()
	s := cmdstats.opmap[opstr]
	cmdstats.RUnlock()

	if s != nil || !create {
		return s
	}

	cmdstats.Lock()
	s = cmdstats.opmap[opstr]
	if s == nil {
		s = &opStats{opstr: opstr}
		for i := 0; i < IntervalNum; i++ {
			s.delayInfo[i] = &delayInfo{interval: IntervalMark[i]}
		}
		cmdstats.opmap[opstr] = s
	}
	cmdstats.Unlock()
	return s
}

type sliceOpStats []*OpStats

func (s sliceOpStats) Len() int {
	return len(s)
}

func (s sliceOpStats) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sliceOpStats) Less(i, j int) bool {
	return s[i].OpStr < s[j].OpStr
}

func GetOpStatsAll() []*OpStats {
	var all = make([]*OpStats, 0, 128)
	cmdstats.RLock()
	for _, s := range cmdstats.opmap {
		all = append(all, s.GetOpStatsByInterval(1))
	}
	cmdstats.RUnlock()
	sort.Sort(sliceOpStats(all))
	return all
}

func GetOpStatsByInterval(interval int64) []*OpStats {
	var all = make([]*OpStats, 0, 128)
	cmdstats.RLock()
	for _, s := range cmdstats.opmap {
		for i := 0; i < IntervalNum; i++ {
			s.RefreshOpStats(i)
		}
		all = append(all, s.GetOpStatsByInterval(interval))
	}
	cmdstats.RUnlock()
	sort.Sort(sliceOpStats(all))
	return all
}

func ResetStats() {
	// Since the session has already obtained the struct from cmdstats.opmap, it cannot be reassigned and can only be reset to zero.
	// Therefore, the command count will not decrease after the reset.
	cmdstats.RLock()
	defer cmdstats.RUnlock()
	for _, v := range cmdstats.opmap {
		v.calls.Set(0)
		v.nsecs.Set(0)
		v.fails.Set(0)
		v.redis.errors.Set(0)
	}

	cmdstats.total.Set(0)
	cmdstats.fails.Set(0)
	cmdstats.redis.errors.Set(0)
	sessions.total.Set(sessions.alive.Int64())
}

func (s *Session) incrOpTotal() {
	s.stats.total.Incr()
}

func incrOpRedisErrors() {
	cmdstats.redis.errors.Incr()
}

func incrOpTotal(n int64) {
	cmdstats.total.Add(n)
}

func incrOpFails(n int64) {
	cmdstats.fails.Add(n)
}

var sessions struct {
	total atomic2.Int64
	alive atomic2.Int64
}

func incrSessions() int64 {
	sessions.total.Incr()
	return sessions.alive.Incr()
}

func decrSessions() {
	sessions.alive.Decr()
}

func SessionsTotal() int64 {
	return sessions.total.Int64()
}

func SessionsAlive() int64 {
	return sessions.alive.Int64()
}

type SysUsage struct {
	Now time.Time
	CPU float64
	*utils.Usage
}

var lastSysUsage atomic.Value

func init() {
	go func() {
		for {
			cpu, usage, err := utils.CPUUsage(time.Second)
			if err != nil {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
				})
			} else {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
					CPU: cpu, Usage: usage,
				})
			}
			if err != nil {
				time.Sleep(time.Second * 5)
			}
		}
	}()
}

func GetSysUsage() *SysUsage {
	if p := lastSysUsage.Load(); p != nil {
		return p.(*SysUsage)
	}
	return nil
}
