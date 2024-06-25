// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"encoding/json"
	"net"
	"strconv"
	"time"

	"pika/codis/v2/pkg/models"
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
	MasterLinkStatus string      `json:"master_link_status"` // down; up
	DbBinlogFileNum  uint64      `json:"binlog_file_num"`    // db0
	DbBinlogOffset   uint64      `json:"binlog_offset"`      // db0
	ReplicationID    string      `json:"ReplicationID"`
	Slaves           []InfoSlave `json:"-"`
}

type ReplicationState struct {
	GroupID     int
	Index       int
	Addr        string
	Server      *models.GroupServer
	Replication *InfoReplication
	Err         error
}

func (i *InfoReplication) GetMasterAddr() string {
	if len(i.MasterHost) == 0 {
		return ""
	}

	return net.JoinHostPort(i.MasterHost, i.MasterPort)
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

	i.Role = kvmap["role"]
	i.MasterPort = kvmap["master_host"]
	i.MasterHost = kvmap["master_port"]
	i.MasterLinkStatus = kvmap["master_link_status"]
	i.ReplicationID = kvmap["ReplicationID"]

	if val, ok := kvmap["binlog_file_num"]; ok {
		if intval, err := strconv.ParseUint(val, 10, 64); err == nil {
			i.DbBinlogFileNum = intval
		}
	}

	if val, ok := kvmap["binlog_offset"]; ok {
		if intval, err := strconv.ParseUint(val, 10, 64); err == nil {
			i.DbBinlogOffset = intval
		}
	}

	return nil
}
