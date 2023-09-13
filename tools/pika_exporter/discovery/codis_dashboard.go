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

type CodisGroupInfo struct {
	Models []CodisModelInfo `json:"models"`
}

type CodisStatsInfo struct {
	Group CodisGroupInfo `json:"group"`
}

type CodisTopomInfo struct {
	Stats CodisStatsInfo `json:"stats"`
}
