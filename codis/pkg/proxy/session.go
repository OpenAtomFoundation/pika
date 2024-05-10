// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Session struct {
	Conn *redis.Conn

	Ops int64

	CreateUnix int64
	LastOpUnix int64

	database int32

	quit bool
	exit sync.Once

	stats struct {
		opmap map[string]*opStats
		total atomic2.Int64
		fails atomic2.Int64
		flush struct {
			n    uint
			nano int64
		}
	}
	start sync.Once

	broken atomic2.Bool
	config *Config
	proxy  *Proxy

	authorized bool
	rand       *rand.Rand
}

func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`
		CreateUnix int64  `json:"create"`
		LastOpUnix int64  `json:"lastop,omitempty"`
		RemoteAddr string `json:"remote"`
	}{
		s.Ops, s.CreateUnix, s.LastOpUnix,
		s.Conn.RemoteAddr(),
	}
	b, _ := json.Marshal(o)
	return string(b)
}

func NewSession(sock net.Conn, config *Config, proxy *Proxy) *Session {
	c := redis.NewConn(sock,
		config.SessionRecvBufsize.AsInt(),
		config.SessionSendBufsize.AsInt(),
	)
	c.ReaderTimeout = config.SessionRecvTimeout.Duration()
	c.WriterTimeout = config.SessionSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.SessionKeepAlivePeriod.Duration())

	s := &Session{
		Conn: c, config: config, proxy: proxy,
		CreateUnix: time.Now().Unix(),
	}
	s.stats.opmap = make(map[string]*opStats, 16)
	s.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	log.Infof("session [%p] create: %s", s, s)
	return s
}

func (s *Session) CloseReaderWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	return s.Conn.CloseReader()
}

func (s *Session) CloseWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	s.broken.Set(true)
	return s.Conn.Close()
}

var (
	ErrRouterNotOnline          = errors.New("router is not online")
	ErrTooManySessions          = errors.New("too many sessions")
	ErrTooManyPipelinedRequests = errors.New("too many pipelined requests")
)

var RespOK = redis.NewString([]byte("OK"))

func (s *Session) Start(d *Router) {
	s.start.Do(func() {
		if int(incrSessions()) > s.config.ProxyMaxClients {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR max number of clients reached"), true)
				s.CloseWithError(ErrTooManySessions)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		if !d.isOnline() {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR router is not online"), true)
				s.CloseWithError(ErrRouterNotOnline)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		tasks := NewRequestChanBuffer(1024)

		go func() {
			s.loopWriter(tasks)
			decrSessions()
		}()

		go func() {
			s.loopReader(tasks, d)
			tasks.Close()
		}()
	})
}

func (s *Session) loopReader(tasks *RequestChan, d *Router) (err error) {
	defer func() {
		s.CloseReaderWithError(err)
	}()

	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)

	for !s.quit {
		multi, err := s.Conn.DecodeMultiBulk()
		if err != nil {
			return err
		}
		if len(multi) == 0 {
			continue
		}
		s.incrOpTotal()

		tasksLen := tasks.Buffered()
		if tasksLen > maxPipelineLen {
			return s.incrOpFails(nil, ErrTooManyPipelinedRequests)
		}

		start := time.Now()
		s.LastOpUnix = start.Unix()
		s.Ops++

		r := &Request{}
		r.Multi = multi
		r.Batch = &sync.WaitGroup{}
		r.Database = s.database
		r.ReceiveTime = start.UnixNano()
		r.TasksLen = int64(tasksLen)

		if err := s.handleRequest(r, d); err != nil {
			r.Resp = redis.NewErrorf("ERR handle request, %s", err)
			tasks.PushBack(r)
			if breakOnFailure {
				return err
			}
		} else {
			tasks.PushBack(r)
		}
	}
	return nil
}

func (s *Session) loopWriter(tasks *RequestChan) (err error) {
	defer func() {
		s.CloseWithError(err)
		tasks.PopFrontAllVoid(func(r *Request) {
			s.incrOpFails(r, nil)
		})
		s.flushOpStats(true)
	}()
	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)
	var cmd = make([]byte, 128)

	p := s.Conn.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = maxPipelineLen / 2

	return tasks.PopFrontAll(func(r *Request) error {
		resp, err := s.handleResponse(r)
		if err != nil {
			resp = redis.NewErrorf("ERR handle response, %s", err)
			if breakOnFailure {
				s.Conn.Encode(resp, true)
				return s.incrOpFails(r, err)
			}
		}
		if err := p.Encode(resp); err != nil {
			return s.incrOpFails(r, err)
		}
		fflush := tasks.IsEmpty()
		if err := p.Flush(fflush); err != nil {
			return s.incrOpFails(r, err)
		} else {
			s.incrOpStats(r, resp.Type)
		}
		//监控响应
		if IsMonitorEnable() && r.Resp != nil && !r.Resp.IsError() {
			delayUs := (time.Now().UnixNano() - r.ReceiveTime) / 1e3
			r.OpFlagMonitor.MonitorResponse(r, s.Conn.RemoteAddr(), delayUs)
			if r.CustomCheckFunc != nil {
				r.CustomCheckFunc.CheckResponse(r, s, delayUs)
			}
		}
		nowTime := time.Now().UnixNano()
		duration := int64((nowTime - r.ReceiveTime) / 1e3)
		s.updateMaxDelay(duration, r)
		if fflush {
			s.flushOpStats(false)
		}
		if duration >= s.config.SlowlogLogSlowerThan {
			SlowCmdCount.Incr() // Atomic global variable, increment by 1 when slow log occurs.
			//client -> proxy -> server -> porxy -> client
			//Record the waiting time from receiving the request from the client to sending it to the backend server
			//the waiting time from sending the request to the backend server to receiving the response from the server
			//the waiting time from receiving the server response to sending it to the client
			var d0, d1, d2 int64 = -1, -1, -1
			if r.SendToServerTime > 0 {
				d0 = int64((r.SendToServerTime - r.ReceiveTime) / 1e3)
			}
			if r.SendToServerTime > 0 && r.ReceiveFromServerTime > 0 {
				d1 = int64((r.ReceiveFromServerTime - r.SendToServerTime) / 1e3)
			}
			if r.ReceiveFromServerTime > 0 {
				d2 = int64((nowTime - r.ReceiveFromServerTime) / 1e3)
			}
			index := getWholeCmd(r.Multi, cmd)
			log.Errorf("%s remote:%s, start_time(us):%d, duration(us): [%d, %d, %d], %d, tasksLen:%d, command:[%s].",
				time.Unix(r.ReceiveTime/1e9, 0).Format("2006-01-02 15:04:05"), s.Conn.RemoteAddr(), r.ReceiveTime/1e3, d0, d1, d2, duration, r.TasksLen, string(cmd[:index]))
		}
		return nil
	})
}

func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
	r.Batch.Wait()
	if r.Coalesce != nil {
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	if err := r.Err; err != nil {
		return nil, err
	} else if r.Resp == nil {
		return nil, ErrRespIsRequired
	}
	return r.Resp, nil
}

func (s *Session) handleRequest(r *Request, d *Router) error {
	opstr, flag, flagMonitor, customCheckFunc, err := getOpInfo(r.Multi)
	if err != nil {
		return err
	}
	r.OpStr = opstr
	r.OpFlag = flag
	r.OpFlagMonitor = flagMonitor
	r.CustomCheckFunc = customCheckFunc
	r.Broken = &s.broken

	if flag.IsNotAllowed() {
		return fmt.Errorf("command '%s' is not allowed", opstr)
	}

	switch opstr {
	case "QUIT":
		return s.handleQuit(r)
	case "AUTH":
		return s.handleAuth(r)
	}

	if !s.authorized {
		if s.config.SessionAuth != "" {
			r.Resp = redis.NewErrorf("NOAUTH Authentication required")
			return nil
		}
		s.authorized = true
	}

	//监控请求
	var isBigRequest bool = false
	if IsMonitorEnable() {
		isBigRequest = flagMonitor.MonitorRequest(r, s.Conn.RemoteAddr())
		if customCheckFunc != nil {
			var customBigCheck = customCheckFunc.CheckRequest(r, s)
			if customBigCheck {
				isBigRequest = true
			}
		}
	}

	switch opstr {
	case "SELECT":
		return s.handleSelect(r)
	case "XMONITOR":
		return s.handleXMonitor(r)
	case "XCONFIG":
		return s.handleXConfig(r)
	case "PING":
		return s.handleRequestPing(r, d)
	case "INFO":
		return s.handleRequestInfo(r, d)
	case "MGET":
		if IfDegradateService(r, isBigRequest, s.rand) { // 熔断降级
			return nil
		}
		return s.handleRequestMGet(r, d)
	case "MSET":
		if IfDegradateService(r, isBigRequest, s.rand) { // 熔断降级
			return nil
		}
		return s.handleRequestMSet(r, d)
	case "DEL":
		if IfDegradateService(r, isBigRequest, s.rand) { // 熔断降级
			return nil
		}
		return s.handleRequestDel(r, d)
	case "EXISTS":
		return s.handleRequestExists(r, d)
	case "PCONFIG":
		return s.handlePConfig(r)
	case "SLOTSINFO":
		return s.handleRequestSlotsInfo(r, d)
	case "SLOTSSCAN":
		return s.handleRequestSlotsScan(r, d)
	case "SLOTSMAPPING":
		return s.handleRequestSlotsMapping(r, d)
	default:
		if IfDegradateService(r, isBigRequest, s.rand) { // 熔断降级
			return nil
		}
		return d.dispatch(r)
	}
}

func (s *Session) handleQuit(r *Request) error {
	s.quit = true
	r.Resp = RespOK
	return nil
}

func (s *Session) handleAuth(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'AUTH' command")
		return nil
	}
	switch {
	case s.config.SessionAuth == "":
		r.Resp = redis.NewErrorf("ERR Client sent AUTH, but no password is set")
	case s.config.SessionAuth != string(r.Multi[1].Value):
		s.authorized = false
		r.Resp = redis.NewErrorf("ERR invalid password")
	default:
		s.authorized = true
		r.Resp = RespOK
	}
	return nil
}

func (s *Session) handleSelect(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SELECT' command")
		return nil
	}
	switch db, err := strconv.Atoi(string(r.Multi[1].Value)); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR invalid DB index")
	case db < 0 || db >= int(s.config.BackendNumberDatabases):
		r.Resp = redis.NewErrorf("ERR invalid DB index, only accept DB [0,%d)", s.config.BackendNumberDatabases)
	default:
		r.Resp = RespOK
		s.database = int32(db)
	}
	return nil
}

// the number of parameters maybe 2, 3, 4
func (s *Session) handleXMonitor(r *Request) error {
	if len(r.Multi) < 2 || len(r.Multi) > 4 {
		r.Resp = redis.NewErrorf("ERR xmonitor parameters")
		return nil
	}
	var subCmd = strings.ToUpper(string(r.Multi[1].Value))
	switch subCmd {
	case "GET", "GETBIGKEY", "GETRISKCMD":
		var recordType int64
		switch subCmd {
		case "GET":
			recordType = MONITOR_GET_ALL
		case "GETBIGKEY":
			recordType = MONITOR_GET_BIG_KEY
		case "GETRISKCMD":
			recordType = MONITOR_GET_RISK_CMD
		default:
			recordType = MONITOR_GET_ALL
		}

		if len(r.Multi) == 3 {
			num, err := strconv.ParseInt(string(r.Multi[2].Value), 10, 64)
			if err != nil {
				r.Resp = redis.NewErrorf("ERR invalid xmonitor number")
				break
			}
			r.Resp = MonitorLogGetByNum(num, recordType)
		} else if len(r.Multi) == 4 {
			var id int64
			var num int64
			var err error
			id, err = strconv.ParseInt(string(r.Multi[2].Value), 10, 64)
			if err != nil {
				r.Resp = redis.NewErrorf("ERR invalid xmonitor start logId")
				break
			}
			num, err = strconv.ParseInt(string(r.Multi[3].Value), 10, 64)
			if err != nil {
				r.Resp = redis.NewErrorf("ERR invalid xmonitor number")
				break
			}

			r.Resp = MonitorLogGetById(id, num, recordType)
		} else {
			r.Resp = MonitorLogGetByNum(10, recordType)
		}
	case "LEN":
		if len(r.Multi) == 2 {
			r.Resp = MonitorLogLen()
		} else {
			r.Resp = redis.NewErrorf("ERR xmonitor parameters")
		}
	case "RESET":
		if len(r.Multi) == 2 {
			r.Resp = MonitorLogReset(false)
		} else if len(r.Multi) == 3 {
			switch strings.ToUpper(string(r.Multi[2].Value)) {
			case "TRUE":
				r.Resp = MonitorLogReset(true)
			case "FALSE":
				r.Resp = MonitorLogReset(false)
			default:
				r.Resp = redis.NewErrorf("ERR xmonitor reset parameters. Try True or False.")
			}
		} else {
			r.Resp = redis.NewErrorf("ERR xmonitor parameters")
		}
	default:
		r.Resp = redis.NewErrorf("ERR Unknown XMONITOR subcommand or wrong args. Try GET|GETBIGKEY|GETRISKCMD, RESET, LEN.")
	}
	return nil
}

// the number of parameters maybe 2, 3, 4
func (s *Session) handleXConfig(r *Request) error {
	if len(r.Multi) < 2 || len(r.Multi) > 4 {
		r.Resp = redis.NewErrorf("ERR xconfig parameters")
		return nil
	}

	var subCmd = strings.ToUpper(string(r.Multi[1].Value))
	switch subCmd {
	case "GET":
		if len(r.Multi) == 3 {
			key := strings.ToLower(string(r.Multi[2].Value))
			r.Resp = s.proxy.ConfigGet(key)
		} else {
			r.Resp = redis.NewErrorf("ERR xconfig get parameters.")
		}
	case "SET":
		//config set *
		if len(r.Multi) == 3 {
			key := strings.ToLower(string(r.Multi[2].Value))
			value := ""
			r.Resp = s.proxy.ConfigSet(key, value)
		} else if len(r.Multi) == 4 {
			key := strings.ToLower(string(r.Multi[2].Value))
			value := string(r.Multi[3].Value)
			r.Resp = s.proxy.ConfigSet(key, value)
		} else {
			r.Resp = redis.NewErrorf("ERR xconfig set parameters.")
		}
	case "REWRITE":
		if len(r.Multi) == 2 {
			r.Resp = s.proxy.ConfigRewrite()
		} else {
			r.Resp = redis.NewErrorf("ERR xconfig rewrite parameters")
		}
	default:
		r.Resp = redis.NewErrorf("ERR Unknown XCONFIG subcommand or wrong args. Try GET, SET, REWRITE.")
	}
	return nil
}

func (s *Session) handleRequestPing(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % uint32(models.GetMaxSlotNum())
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % uint32(models.GetMaxSlotNum())
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestMGet(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MGET' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var array = make([]*redis.Resp, len(sub))
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsArray() && len(resp.Array) == 1:
				array[i] = resp.Array[0]
			default:
				return fmt.Errorf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array))
			}
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	return nil
}

func (s *Session) handleRequestMSet(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0 || nblks%2 != 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MSET' command")
		return nil
	case nblks == 2:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nblks / 2)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i*2+1],
			r.Multi[i*2+2],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsString():
				r.Resp = resp
			default:
				return fmt.Errorf("bad mset resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		return nil
	}
	return nil
}

func (s *Session) handleRequestDel(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'DEL' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				n += int(resp.Value[0] - '0')
			default:
				return fmt.Errorf("bad del resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestExists(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'EXISTS' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				if resp.Value[0] != '0' {
					n++
				}
			default:
				return fmt.Errorf("bad exists resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks != 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSINFO' command")
		return nil
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsScan(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks <= 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSSCAN' command")
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= int64(models.GetMaxSlotNum()):
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		return d.dispatchSlot(r, int(slot))
	}
}

func (s *Session) handleRequestSlotsMapping(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks >= 2:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSMAPPING' command")
		return nil
	}
	marshalToResp := func(m *models.Slot) *redis.Resp {
		if m == nil {
			return redis.NewArray(nil)
		}
		var replicaGroups []*redis.Resp
		for i := range m.ReplicaGroups {
			var group []*redis.Resp
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, redis.NewString([]byte(addr)))
			}
			replicaGroups = append(replicaGroups, redis.NewArray(group))
		}
		return redis.NewArray([]*redis.Resp{
			redis.NewString([]byte(strconv.Itoa(m.Id))),
			redis.NewString([]byte(m.BackendAddr)),
			redis.NewString([]byte(m.MigrateFrom)),
			redis.NewArray(replicaGroups),
		})
	}
	if nblks == 0 {
		var array = make([]*redis.Resp, uint32(models.GetMaxSlotNum()))
		for i, m := range d.GetSlots() {
			array[i] = marshalToResp(m)
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= int64(models.GetMaxSlotNum()):
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		r.Resp = marshalToResp(d.GetSlot(int(slot)))
		return nil
	}
}

func (s *Session) incrOpTotal() {
	s.stats.total.Incr()
}

func (s *Session) getOpStats(opstr string) *opStats {
	e := s.stats.opmap[opstr]
	if e == nil {
		e = &opStats{opstr: opstr}
		s.stats.opmap[opstr] = e
	}
	return e
}

func (s *Session) incrOpStats(r *Request, t redis.RespType) {
	e := s.getOpStats(r.OpStr)
	e.calls.Incr()
	e.nsecs.Add(time.Now().UnixNano() - r.ReceiveTime)
	switch t {
	case redis.TypeError:
		e.redis.errors.Incr()
	}
}

func (s *Session) incrOpFails(r *Request, err error) error {
	if r != nil {
		e := s.getOpStats(r.OpStr)
		e.fails.Incr()
	} else {
		s.stats.fails.Incr()
	}
	return err
}

func (s *Session) flushOpStats(force bool) {
	var nano = time.Now().UnixNano()
	if !force {
		const period = int64(time.Millisecond) * 100
		if d := nano - s.stats.flush.nano; d < period {
			return
		}
	}
	s.stats.flush.nano = nano

	incrOpTotal(s.stats.total.Swap(0))
	incrOpFails(s.stats.fails.Swap(0))
	for _, e := range s.stats.opmap {
		if e.calls.Int64() != 0 || e.fails.Int64() != 0 {
			incrOpStats(e)
		}
	}
	s.stats.flush.n++

	if len(s.stats.opmap) <= 32 {
		return
	}
	if (s.stats.flush.n % 16384) == 0 {
		s.stats.opmap = make(map[string]*opStats, 32)
	}
}

func (s *Session) handlePConfig(r *Request) error {
	if len(r.Multi) < 2 || len(r.Multi) > 4 {
		r.Resp = redis.NewErrorf("ERR config parameters")
		return nil
	}

	var subCmd = strings.ToUpper(string(r.Multi[1].Value))
	switch subCmd {
	case "GET":
		if len(r.Multi) == 3 {
			key := strings.ToLower(string(r.Multi[2].Value))
			r.Resp = s.proxy.ConfigGet(key)
		} else {
			r.Resp = redis.NewErrorf("ERR config get parameters.")
		}
	case "SET":
		if len(r.Multi) == 3 {
			key := strings.ToLower(string(r.Multi[2].Value))
			value := ""
			r.Resp = s.proxy.ConfigSet(key, value)
		} else if len(r.Multi) == 4 {
			key := strings.ToLower(string(r.Multi[2].Value))
			value := string(r.Multi[3].Value)
			r.Resp = s.proxy.ConfigSet(key, value)
		} else {
			r.Resp = redis.NewErrorf("ERR config set parameters.")
		}
	case "REWRITE":
		if len(r.Multi) == 2 {
			r.Resp = s.proxy.ConfigRewrite()
		} else {
			r.Resp = redis.NewErrorf("ERR config rewrite parameters")
		}
	default:
		r.Resp = redis.NewErrorf("ERR Unknown CONFIG subcommand or wrong args. Try GET, SET, REWRITE.")
	}
	return nil
}

func (s *Session) updateMaxDelay(duration int64, r *Request) {
	e := s.getOpStats(r.OpStr) // There is no race condition in the session
	if duration > e.maxDelay.Int64() {
		e.maxDelay.Set(duration)
	}
}
