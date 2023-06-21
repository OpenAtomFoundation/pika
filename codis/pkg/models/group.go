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

	// master 或是 slave
	Role string
	// 如果是master节点，取 master_repl_offset 字段，否则取 slave_repl_offset 字段
	ReplyOffset int
	// 监控状态，分为：0 正常，1 主观下线，2 实际下线
	// 如果被标记为2，则不提供服务
	State GroupServerState `json:"state"`

	ReCallTimes int8 `json:"recall_times"`

	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
