// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/math2"
	"pika/codis/v2/pkg/utils/rpc"
	"pika/codis/v2/pkg/utils/unsafe2"
)

type Proxy struct {
	mu sync.Mutex

	xauth string
	model *models.Proxy

	exit struct {
		C chan struct{}
	}
	online bool
	closed bool

	config *Config
	router *Router
	ignore []byte

	lproxy net.Listener
	ladmin net.Listener

	ha struct {
		masters map[int]string
		servers []string
	}
	jodis *Jodis
}

var ErrClosedProxy = errors.New("use of closed proxy")

func New(config *Config) (*Proxy, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}

	p := &Proxy{}
	p.config = config
	p.exit.C = make(chan struct{})
	p.router = NewRouter(config)
	p.ignore = make([]byte, config.ProxyHeapPlaceholder.Int64())

	p.model = &models.Proxy{
		StartTime: time.Now().String(),
	}
	p.model.ProductName = config.ProductName
	p.model.DataCenter = config.ProxyDataCenter
	p.model.Pid = os.Getpid()
	p.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		p.model.Sys = strings.TrimSpace(string(b))
	}
	p.model.Hostname = utils.Hostname

	if err := p.setup(config); err != nil {
		p.Close()
		return nil, err
	}

	log.Warnf("[%p] create new proxy:\n%s", p, p.model.Encode())

	unsafe2.SetMaxOffheapBytes(config.ProxyMaxOffheapBytes.Int64())

	go p.serveAdmin()
	go p.serveProxy()

	p.startMetricsJson()
	p.startMetricsInfluxdb()
	p.startMetricsStatsd()

	return p, nil
}

func (p *Proxy) setup(config *Config) error {
	proto := config.ProtoType

	var l net.Listener
	var err error

	if config.ProxyTLS {
		log.Printf("TLS configuration: cert = %s, key = %s", config.ProxyTLSCert, config.ProxyTLSKey)

		// Load the TLS certificate
		cert, err := tls.LoadX509KeyPair(config.ProxyTLSCert, config.ProxyTLSKey)
		if err != nil {
			return errors.Trace(err)
		}

		// Set up TLS configuration to use TLS 1.2 or higher
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		// Create a TLS listener
		l, err = tls.Listen(proto, config.ProxyAddr, tlsConfig)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Printf("Setting up a non-TLS listener")

		// Create a non-TLS listener
		l, err = net.Listen(proto, config.ProxyAddr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Common setup for both TLS and non-TLS
	p.lproxy = l

	// Replace unspecified IP address with the specific host proxy address
	x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostProxy)
	if err != nil {
		log.Errorf("Failed to replace unspecified IP: %s", err)
		return err
	}

	// Update proxy model
	p.model.ProtoType = proto
	p.model.ProxyAddr = x


	proto = "tcp"
	if l, err := net.Listen(proto, config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		p.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostAdmin)
		if err != nil {
			return err
		}
		p.model.AdminAddr = x
	}

	p.model.Token = rpc.NewToken(
		config.ProductName,
		p.lproxy.Addr().String(),
		p.ladmin.Addr().String(),
	)
	p.xauth = rpc.NewXAuth(
		config.ProductName,
		config.ProductAuth,
		p.model.Token,
	)

	if config.JodisAddr != "" {
		c, err := models.NewClient(config.JodisName, config.JodisAddr, config.JodisAuth, config.JodisTimeout.Duration())
		if err != nil {
			return err
		}
		if config.JodisCompatible {
			p.model.JodisPath = filepath.Join("/zk/codis", fmt.Sprintf("db_%s", config.ProductName), "proxy", p.model.Token)
		} else {
			p.model.JodisPath = models.JodisPath(config.ProductName, p.model.Token)
		}
		p.jodis = NewJodis(c, p.model)
	}
	p.model.MaxSlotNum = config.MaxSlotNum

	return nil
}

func (p *Proxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedProxy
	}
	if p.online {
		return nil
	}
	p.online = true
	p.router.Start()
	if p.jodis != nil {
		p.jodis.Start()
	}
	return nil
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.exit.C)

	if p.jodis != nil {
		p.jodis.Close()
	}
	if p.ladmin != nil {
		p.ladmin.Close()
	}
	if p.lproxy != nil {
		p.lproxy.Close()
	}
	if p.router != nil {
		p.router.Close()
	}
	return nil
}

func (p *Proxy) XAuth() string {
	return p.xauth
}

func (p *Proxy) Model() *models.Proxy {
	return p.model
}

func (p *Proxy) Config() *Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config
}

func (p *Proxy) ConfigGet(key string) *redis.Resp {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch key {
	case "jodis":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("jodis_name")),
			redis.NewBulkBytes([]byte(p.config.JodisName)),
			redis.NewBulkBytes([]byte("jodis_addr")),
			redis.NewBulkBytes([]byte(p.config.JodisAddr)),
			redis.NewBulkBytes([]byte("jodis_auth")),
			redis.NewBulkBytes([]byte(p.config.JodisAuth)),
			redis.NewBulkBytes([]byte("jodis_timeout")),
			redis.NewBulkBytes([]byte(p.config.JodisTimeout.Duration().String())),
			redis.NewBulkBytes([]byte("jodis_compatible")),
			redis.NewBulkBytes([]byte(strconv.FormatBool(p.config.JodisCompatible))),
		})
	case "proxy":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("proxy_datacenter")),
			redis.NewBulkBytes([]byte(p.config.ProxyDataCenter)),
			redis.NewBulkBytes([]byte("proxy_max_clients")),
			redis.NewBulkBytes([]byte(strconv.Itoa(p.config.ProxyMaxClients))),
			redis.NewBulkBytes([]byte("proxy_max_offheap_size")),
			redis.NewBulkBytes([]byte(p.config.ProxyMaxOffheapBytes.HumanString())),
			redis.NewBulkBytes([]byte("proxy_heap_placeholder")),
			redis.NewBulkBytes([]byte(p.config.ProxyHeapPlaceholder.HumanString())),
		})
	case "backend_ping_period":
		return redis.NewBulkBytes([]byte(p.config.BackendPingPeriod.Duration().String()))
	case "backend_buffer_size":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("backend_recv_bufsize")),
			redis.NewBulkBytes([]byte(p.config.BackendRecvBufsize.HumanString())),
			redis.NewBulkBytes([]byte("backend_send_bufsize")),
			redis.NewBulkBytes([]byte(p.config.BackendSendBufsize.HumanString())),
		})
	case "backend_timeout":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("backend_recv_timeout")),
			redis.NewBulkBytes([]byte(p.config.BackendRecvTimeout.Duration().String())),
			redis.NewBulkBytes([]byte("backend_send_timeout")),
			redis.NewBulkBytes([]byte(p.config.BackendSendTimeout.Duration().String())),
		})
	case "backend_max_pipeline":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.BackendMaxPipeline)))
	case "backend_primary_only":
		return redis.NewBulkBytes([]byte(strconv.FormatBool(p.config.BackendPrimaryOnly)))
	case "max_slot_num":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.MaxSlotNum)))
	case "backend_primary_parallel":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.BackendPrimaryParallel)))
	case "backend_replica_parallel":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.BackendReplicaParallel)))
	case "backend_primary_quick":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.BackendPrimaryQuick)))
	case "backend_replica_quick":
		return redis.NewBulkBytes([]byte(strconv.Itoa(p.config.BackendReplicaQuick)))
	case "backend_keepalive_period":
		return redis.NewBulkBytes([]byte(p.config.BackendKeepAlivePeriod.Duration().String()))
	case "backend_number_databases":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(int64(p.config.BackendNumberDatabases), 10)))
	case "session_buffer_size":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("session_recv_bufsize")),
			redis.NewBulkBytes([]byte(p.config.SessionRecvBufsize.HumanString())),
			redis.NewBulkBytes([]byte("session_send_bufsize")),
			redis.NewBulkBytes([]byte(p.config.SessionSendBufsize.HumanString())),
		})
	case "session_timeout":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("session_recv_timeout")),
			redis.NewBulkBytes([]byte(p.config.SessionRecvTimeout.Duration().String())),
			redis.NewBulkBytes([]byte("session_send_timeout")),
			redis.NewBulkBytes([]byte(p.config.SessionSendTimeout.Duration().String())),
		})
	case "slowlog_log_slower_than":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(p.config.SlowlogLogSlowerThan, 10)))
	case "metrics_report_server":
		return redis.NewBulkBytes([]byte(p.config.MetricsReportServer))
	case "metrics_report_period":
		return redis.NewBulkBytes([]byte(p.config.MetricsReportPeriod.Duration().String()))
	case "metrics_report_influxdb":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("metrics_report_influxdb_server")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportInfluxdbServer)),
			redis.NewBulkBytes([]byte("metrics_report_influxdb_period")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportInfluxdbPeriod.Duration().String())),
			redis.NewBulkBytes([]byte("metrics_report_influxdb_username")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportInfluxdbUsername)),
			redis.NewBulkBytes([]byte("metrics_report_influxdb_database")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportInfluxdbDatabase)),
		})
	case "metrics_report_statsd":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("metrics_report_statsd_server")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportStatsdServer)),
			redis.NewBulkBytes([]byte("metrics_report_statsd_period")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportStatsdPeriod.Duration().String())),
			redis.NewBulkBytes([]byte("metrics_report_statsd_prefix")),
			redis.NewBulkBytes([]byte(p.config.MetricsReportStatsdPrefix)),
		})
	case "quick_cmd_list":
		return redis.NewBulkBytes([]byte(p.config.QuickCmdList))
	case "slow_cmd_list":
		return redis.NewBulkBytes([]byte(p.config.SlowCmdList))
	case "quick_slow_cmd":
		return getCmdFlag()
	case "max_delay_refresh_time_interval":
		if text, err := p.config.MaxDelayRefreshTimeInterval.MarshalText(); err != nil {
			return redis.NewErrorf("cant get max_delay_refresh_time_interval value.")
		} else {
			return redis.NewBulkBytes(text)
		}
	default:
		return redis.NewErrorf("unsupported key: %s", key)
	}
}

func (p *Proxy) ConfigSet(key, value string) *redis.Resp {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch key {
	case "product_name":
		p.config.ProductName = value
		return redis.NewString([]byte("OK"))
	case "proxy_max_clients":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s", err)
		}
		if n <= 0 {
			return redis.NewErrorf("invalid proxy_max_clients")
		}
		p.config.ProxyMaxClients = n
		return redis.NewString([]byte("OK"))
	case "backend_primary_only":
		return redis.NewErrorf("not currently supported")
	case "slowlog_log_slower_than":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s", err)
		}
		if n < 0 {
			return redis.NewErrorf("invalid slowlog_log_slower_than")
		}
		p.config.SlowlogLogSlowerThan = n
		return redis.NewString([]byte("OK"))
	case "quick_cmd_list":
		err := setCmdListFlag(value, FlagQuick)
		if err != nil {
			log.Warnf("setQuickCmdList config[%s] failed, recover old config[%s].", value, p.config.QuickCmdList)
			setCmdListFlag(p.config.QuickCmdList, FlagQuick)
			return redis.NewErrorf("err：%s.", err)
		}
		p.config.QuickCmdList = value
		return redis.NewString([]byte("OK"))
	case "slow_cmd_list":
		err := setCmdListFlag(value, FlagSlow)
		if err != nil {
			log.Warnf("setSlowCmdList config[%s] failed, recover old config[%s].", value, p.config.SlowCmdList)
			setCmdListFlag(p.config.SlowCmdList, FlagSlow)
			return redis.NewErrorf("err：%s.", err)
		}
		p.config.SlowCmdList = value
		return redis.NewString([]byte("OK"))
	case "backend_replica_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if n < 0 || n >= p.config.BackendReplicaParallel {
			return redis.NewErrorf("invalid backend_replica_quick")
		} else {
			p.config.BackendReplicaQuick = n
			p.router.SetReplicaQuickConn(p.config.BackendReplicaQuick)
			return redis.NewString([]byte("OK"))
		}
	case "backend_primary_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if n < 0 || n >= p.config.BackendPrimaryParallel {
			return redis.NewErrorf("invalid backend_primary_quick")
		} else {
			p.config.BackendPrimaryQuick = n
			p.router.SetPrimaryQuickConn(p.config.BackendPrimaryQuick)
			return redis.NewString([]byte("OK"))
		}
	case "max_delay_refresh_time_interval":
		s := &(p.config.MaxDelayRefreshTimeInterval)
		err := s.UnmarshalText([]byte(value))
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if d := p.config.MaxDelayRefreshTimeInterval.Duration(); d <= 0 {
			return redis.NewErrorf("max_delay_refresh_time_interval must be greater than 0")
		} else {
			RefreshPeriod.Set(int64(d))
			return redis.NewString([]byte("OK"))
		}
	default:
		return redis.NewErrorf("unsupported key: %s", key)
	}
}

func (p *Proxy) ConfigRewrite() *redis.Resp {
	p.mu.Lock()
	defer p.mu.Unlock()
	utils.RewriteConfig(*(p.config), p.config.ConfigFileName, "=", true)
	return redis.NewString([]byte("OK"))
}

func (p *Proxy) IsOnline() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.online && !p.closed
}

func (p *Proxy) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Proxy) HasSwitched() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.router.HasSwitched()
}

func (p *Proxy) Slots() []*models.Slot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.router.GetSlots()
}

func (p *Proxy) FillSlot(m *models.Slot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedProxy
	}
	return p.router.FillSlot(m)
}

func (p *Proxy) FillSlots(slots []*models.Slot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedProxy
	}
	for _, m := range slots {
		if err := p.router.FillSlot(m); err != nil {
			return err
		}
	}
	return nil
}

func (p *Proxy) SwitchMasters(masters map[int]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedProxy
	}
	p.ha.masters = masters

	if len(masters) != 0 {
		p.router.SwitchMasters(masters)
	}
	return nil
}

func (p *Proxy) GetSentinels() ([]string, map[int]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil
	}
	return p.ha.servers, p.ha.masters
}

func (p *Proxy) serveAdmin() {
	if p.IsClosed() {
		return
	}
	defer p.Close()

	log.Warnf("[%p] admin start service on %s", p, p.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(p))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(p.ladmin)

	select {
	case <-p.exit.C:
		log.Warnf("[%p] admin shutdown", p)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] admin exit on error", p)
	}
}

func (p *Proxy) serveProxy() {
	if p.IsClosed() {
		return
	}
	defer p.Close()

	log.Warnf("[%p] proxy start service on %s", p, p.lproxy.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) (err error) {
		defer func() {
			eh <- err
		}()
		for {
			c, err := p.acceptConn(l)
			if err != nil {
				return err
			}
			NewSession(c, p.config, p).Start(p.router)
		}
	}(p.lproxy)

	if d := p.config.BackendPingPeriod.Duration(); d != 0 {
		go p.keepAlive(d)
	}

	if err := setCmdListFlag(p.config.QuickCmdList, FlagQuick); err != nil {
		log.PanicErrorf(err, "setQuickCmdList [%s] failed", p.config.QuickCmdList)
	}
	if err := setCmdListFlag(p.config.SlowCmdList, FlagSlow); err != nil {
		log.PanicErrorf(err, "setSlowCmdList [%s] failed", p.config.SlowCmdList)
	}

	select {
	case <-p.exit.C:
		log.Warnf("[%p] proxy shutdown", p)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] proxy exit on error", p)
	}
}

func (p *Proxy) keepAlive(d time.Duration) {
	var ticker = time.NewTicker(math2.MaxDuration(d, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-p.exit.C:
			return
		case <-ticker.C:
			p.router.KeepAlive()
		}
	}
}

func (p *Proxy) acceptConn(l net.Listener) (net.Conn, error) {
	var delay = &DelayExp2{
		Min: 10, Max: 500,
		Unit: time.Millisecond,
	}
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed", p)
				delay.Sleep()
				continue
			}
		}
		return c, err
	}
}

type Overview struct {
	Version string         `json:"version"`
	Compile string         `json:"compile"`
	Config  *Config        `json:"config,omitempty"`
	Model   *models.Proxy  `json:"model,omitempty"`
	Stats   *Stats         `json:"stats,omitempty"`
	Slots   []*models.Slot `json:"slots,omitempty"`
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	Sentinels struct {
		Servers  []string          `json:"servers,omitempty"`
		Masters  map[string]string `json:"masters,omitempty"`
		Switched bool              `json:"switched,omitempty"`
	} `json:"sentinels"`

	Ops struct {
		Total int64 `json:"total"`
		Fails int64 `json:"fails"`
		Redis struct {
			Errors int64 `json:"errors"`
		} `json:"redis"`
		QPS int64      `json:"qps"`
		Cmd []*OpStats `json:"cmd,omitempty"`
	} `json:"ops"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string       `json:"now"`
		CPU float64      `json:"cpu"`
		Mem int64        `json:"mem"`
		Raw *utils.Usage `json:"raw,omitempty"`
	} `json:"rusage"`

	Backend struct {
		PrimaryOnly bool `json:"primary_only"`
	} `json:"backend"`

	Runtime      *RuntimeStats `json:"runtime,omitempty"`
	SlowCmdCount int64         `json:"slow_cmd_count"` // Cumulative count of slow log
}

type RuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Lookups uint64 `json:"lookups"`
		Mallocs uint64 `json:"mallocs"`
		Frees   uint64 `json:"frees"`
	} `json:"general"`

	Heap struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Idle    uint64 `json:"idle"`
		Inuse   uint64 `json:"inuse"`
		Objects uint64 `json:"objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"num"`
		CPUFraction  float64 `json:"cpu_fraction"`
		TotalPauseMs uint64  `json:"total_pausems"`
	} `json:"gc"`

	NumProcs      int   `json:"num_procs"`
	NumGoroutines int   `json:"num_goroutines"`
	NumCgoCall    int64 `json:"num_cgo_call"`
	MemOffheap    int64 `json:"mem_offheap"`
}

type StatsFlags uint32

func (s StatsFlags) HasBit(m StatsFlags) bool {
	return (s & m) != 0
}

const (
	StatsCmds = StatsFlags(1 << iota)
	StatsSlots
	StatsRuntime

	StatsFull = StatsFlags(^uint32(0))
)

func (p *Proxy) Overview(flags StatsFlags) *Overview {
	o := &Overview{
		Version: utils.Version,
		Compile: utils.Compile,
		Config:  p.Config(),
		Model:   p.Model(),
		Stats:   p.Stats(flags),
	}
	if flags.HasBit(StatsSlots) {
		o.Slots = p.Slots()
	}
	return o
}

func (p *Proxy) Stats(flags StatsFlags) *Stats {
	stats := &Stats{}
	stats.Online = p.IsOnline()
	stats.Closed = p.IsClosed()

	stats.Ops.Total = OpTotal()
	stats.Ops.Fails = OpFails()
	stats.Ops.Redis.Errors = OpRedisErrors()
	stats.Ops.QPS = OpQPS()

	if flags.HasBit(StatsCmds) {
		stats.Ops.Cmd = GetOpStatsAll()
	}

	stats.Sessions.Total = SessionsTotal()
	stats.Sessions.Alive = SessionsAlive()

	if u := GetSysUsage(); u != nil {
		stats.Rusage.Now = u.Now.String()
		stats.Rusage.CPU = u.CPU
		stats.Rusage.Mem = u.MemTotal()
		stats.Rusage.Raw = u.Usage
	}

	stats.Backend.PrimaryOnly = p.Config().BackendPrimaryOnly

	if flags.HasBit(StatsRuntime) {
		var r runtime.MemStats
		runtime.ReadMemStats(&r)

		stats.Runtime = &RuntimeStats{}
		stats.Runtime.General.Alloc = r.Alloc
		stats.Runtime.General.Sys = r.Sys
		stats.Runtime.General.Lookups = r.Lookups
		stats.Runtime.General.Mallocs = r.Mallocs
		stats.Runtime.General.Frees = r.Frees
		stats.Runtime.Heap.Alloc = r.HeapAlloc
		stats.Runtime.Heap.Sys = r.HeapSys
		stats.Runtime.Heap.Idle = r.HeapIdle
		stats.Runtime.Heap.Inuse = r.HeapInuse
		stats.Runtime.Heap.Objects = r.HeapObjects
		stats.Runtime.GC.Num = r.NumGC
		stats.Runtime.GC.CPUFraction = r.GCCPUFraction
		stats.Runtime.GC.TotalPauseMs = r.PauseTotalNs / uint64(time.Millisecond)
		stats.Runtime.NumProcs = runtime.GOMAXPROCS(0)
		stats.Runtime.NumGoroutines = runtime.NumGoroutine()
		stats.Runtime.NumCgoCall = runtime.NumCgoCall()
		stats.Runtime.MemOffheap = unsafe2.OffheapBytes()
	}
	stats.SlowCmdCount = SlowCmdCount.Int64()
	return stats
}
