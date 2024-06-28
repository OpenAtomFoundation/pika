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

// SelectNewMaster choose a new master node in the group
func (g *Group) SelectNewMaster() (string, int) {
	var newMasterServer *GroupServer
	var newMasterIndex = -1

	for index, server := range g.Servers {
		if index == 0 || server.State != GroupServerStateNormal || !server.IsEligibleForMasterElection {
			continue
		}

		if newMasterServer == nil {
			newMasterServer = server
			newMasterIndex = index
		} else if server.DbBinlogFileNum > newMasterServer.DbBinlogFileNum {
			// Select the slave node with the latest offset as the master node
			newMasterServer = server
			newMasterIndex = index
		} else if server.DbBinlogFileNum == newMasterServer.DbBinlogFileNum {
			if server.DbBinlogOffset > newMasterServer.DbBinlogOffset {
				newMasterServer = server
				newMasterIndex = index
			}
		}
	}

	if newMasterServer == nil {
		return "", newMasterIndex
	}

	return newMasterServer.Addr, newMasterIndex
}

type GroupServerState int8

const (
	GroupServerStateNormal GroupServerState = iota
	GroupServerStateSubjectiveOffline
	GroupServerStateOffline
)

type GroupServerRole string

const (
	RoleMaster GroupServerRole = "master"
	RoleSlave  GroupServerRole = "slave"
)

type GroupServer struct {
	Addr       string `json:"server"`
	DataCenter string `json:"datacenter"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`

	// master or slave
	Role GroupServerRole `json:"role"`
	// If it is a master node, take the master_repl_offset field, otherwise take the slave_repl_offset field
	DbBinlogFileNum             uint64 `json:"binlog_file_num"` // db0
	DbBinlogOffset              uint64 `json:"binlog_offset"`   // db0
	IsEligibleForMasterElection bool   `json:"is_eligible_for_master_election"`

	// Monitoring status, 0 normal, 1 subjective offline, 2 actual offline
	// If marked as 2 , no service is provided
	State GroupServerState `json:"state"`

	ReCallTimes int8 `json:"recall_times"`

	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
