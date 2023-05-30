// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"encoding/json"
	"strconv"
	"time"
)

type SentinelMaster struct {
	Addr  string
	Info  map[string]string
	Epoch int64
}

type MonitorConfig struct {
	Quorum          int
	ParallelSyncs   int
	DownAfter       time.Duration
	FailoverTimeout time.Duration

	NotificationScript   string
	ClientReconfigScript string
}

type SentinelGroup struct {
	Master map[string]string   `json:"master"`
	Slaves []map[string]string `json:"slaves,omitempty"`
}

type InfoSlave struct {
	IP     string `json:"ip"`
	Port   string `json:"port"`
	State  string `json:"state"`
	Offset int    `json:"offset"`
	Lag    int    `json:"lag"`
}

func (i *InfoSlave) UnmarshalJSON(b []byte) error {
	var kvmap map[string]string
	if err := json.Unmarshal(b, &kvmap); err != nil {
		return err
	}

	i.IP = kvmap["ip"]
	i.Port = kvmap["port"]
	i.State = kvmap["state"]

	if val, ok := kvmap["offset"]; ok {
		if intval, err := strconv.Atoi(val); err == nil {
			i.Offset = intval
		}
	}
	if val, ok := kvmap["lag"]; ok {
		if intval, err := strconv.Atoi(val); err == nil {
			i.Lag = intval
		}
	}
	return nil
}

type InfoReplication struct {
	Role             string      `json:"role"`
	ConnectedSlaves  int         `json:"connected_slaves"`
	MasterHost       string      `json:"master_host"`
	MasterPort       string      `json:"master_port"`
	SlaveReplOffset  int         `json:"slave_repl_offset"`
	MasterReplOffset int         `json:"master_repl_offset"`
	Slaves           []InfoSlave `json:"-"`
}

type ReplicationState struct {
	GroupID     int
	Index       int
	Addr        string
	Replication *InfoReplication
	Err         error
}

func (i *InfoReplication) UnmarshalJSON(b []byte) error {
	var kvmap map[string]string
	if err := json.Unmarshal(b, &kvmap); err != nil {
		return err
	}

	if val, ok := kvmap["connected_slaves"]; ok {
		if intval, err := strconv.Atoi(val); err == nil {
			i.ConnectedSlaves = intval
		}
	}
	if val, ok := kvmap["slave_repl_offset"]; ok {
		if intval, err := strconv.Atoi(val); err == nil {
			i.SlaveReplOffset = intval
		}
	}
	if val, ok := kvmap["master_repl_offset"]; ok {
		if intval, err := strconv.Atoi(val); err == nil {
			i.MasterReplOffset = intval
		}
	}
	i.Role = kvmap["role"]
	i.MasterPort = kvmap["master_host"]
	i.MasterHost = kvmap["master_port"]
	return nil
}
