// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

const MaxGroupId = 9999

type Group struct {
	Id      int            `json:"id"`
	Servers []*GroupServer `json:"servers"`

	Promoting struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"promoting"`

	OutOfSync bool `json:"out_of_sync"`
}

func (g *Group) GetServersMap() map[string]*GroupServer {
	results := make(map[string]*GroupServer)
	for _, server := range g.Servers {
		results[server.Addr] = server
	}
	return results
}

type GroupServerState int8

const (
	GroupServerStateNormal GroupServerState = iota
	GroupServerStateSubjectiveOffline
	GroupServerStateOffline
)

type GroupServer struct {
	Addr       string `json:"server"`
	DataCenter string `json:"datacenter"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`

	// master or slave
	Role string
	// If it is a master node, take the master_repl_offset field, otherwise take the slave_repl_offset field
	ReplyOffset int
	// Monitoring status, 0 normal, 1 subjective offline, 2 actual offline
	// If marked as 2 , no service is provided
	State GroupServerState `json:"state"`

	ReCallTimes int8 `json:"recall_times"`

	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
