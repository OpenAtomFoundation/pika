package discovery

type CodisServerAction struct {
	Action string `json:"action"`
}

type CodisServerInfo struct {
	Server             string            `json:"server"`
	Datacenter         string            `json:"datacenter"`
	ServerAction       CodisServerAction `json:"action"`
	ServerReplicaGroup bool              `json:"replica_group"`
}

type CodisModelInfo struct {
	Id      int               `json:"id"`
	Servers []CodisServerInfo `json:"servers"`
}

type CodisProxyModelInfo struct {
	Id          int    `json:"id"`
	AdminAddr   string `json:"admin_addr"`
	ProductName string `json:"product_name"`
	DataCenter  string `json:"data_center"`
}

type CodisGroupInfo struct {
	Models []CodisModelInfo `json:"models"`
}

type CodisProxyInfo struct {
	Models []CodisProxyModelInfo `json:"models"`
}

type CodisStatsInfo struct {
	Group CodisGroupInfo `json:"group"`
	Proxy CodisProxyInfo `json:"proxy"`
}

type CodisTopomInfo struct {
	Stats CodisStatsInfo `json:"stats"`
}

type RedisInfo struct {
	Errors int `json:"errors"`
}

type CmdInfo struct {
	Opstr         string `json:"opstr"`
	Calls         int64  `json:"calls"`
	Usecs_percall int64  `json:"usecs_percall"`
	Fails         int64  `json:"fails"`
	MaxDelay      int64  `json:"max_delay"`
	AVG           int64  `json:"avg"`
	TP90          int64  `json:"tp90"`
	TP99          int64  `json:"tp99"`
	TP999         int64  `json:"tp999"`
	TP9999        int64  `json:"tp9999"`
	TP100         int64  `json:"tp100"`

	Delay50ms  int64 `json:"delay50ms"`
	Delay100ms int64 `json:"delay100ms"`
	Delay200ms int64 `json:"delay200ms"`
	Delay300ms int64 `json:"delay300ms"`
	Delay500ms int64 `json:"delay500ms"`
	Delay1s    int64 `json:"delay1s"`
	Delay2s    int64 `json:"delay2s"`
	Delay3s    int64 `json:"delay3s"`
}

type ProxyOpsInfo struct {
	Total int       `json:"total"`
	Fails int       `json:"fails"`
	Redis RedisInfo `json:"redis"`
	Qps   int       `json:"qps"`
	Cmd   []CmdInfo `json:"cmd"`
}

type RowInfo struct {
	Utime  int64 `json:"utime"`
	Stime  int64 `json:"stime"`
	MaxRss int64 `json:"max_rss"`
	IxRss  int64 `json:"ix_rss"`
	IdRss  int64 `json:"id_rss"`
	IsRss  int64 `json:"is_rss"`
}

type RusageInfo struct {
	Now string  `json:"now"`
	Cpu float64 `json:"cpu"`
	Mem float64 `json:"mem"`
	Raw RowInfo `json:"raw"`
}

type GeneralInfo struct {
	Alloc   int64 `json:"alloc"`
	Sys     int64 `json:"sys"`
	Lookups int64 `json:"lookups"`
	Mallocs int64 `json:"mallocs"`
	Frees   int64 `json:"frees"`
}

type HeapInfo struct {
	Alloc   int64 `json:"alloc"`
	Sys     int64 `json:"sys"`
	Idle    int64 `json:"idle"`
	Inuse   int64 `json:"inuse"`
	Objects int64 `json:"objects"`
}

type RunTimeInfo struct {
	General GeneralInfo `json:"general"`
	Heap    HeapInfo    `json:"heap"`
}

type ProxyStats struct {
	Online       bool         `json:"online"`
	Ops          ProxyOpsInfo `json:"ops"`
	Rusage       RusageInfo   `json:"rusage"`
	RunTime      RunTimeInfo  `json:"runtime"`
	SlowCmdCount int64        `json:"slow_cmd_count"`
}
