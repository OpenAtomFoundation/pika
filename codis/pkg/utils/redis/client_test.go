package redis

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMasterInfoReplication(t *testing.T) {
	text := `
# Replication(MASTER)
role:master
ReplicationID:94e8feeaf9036a77c59ad2f091f1c0b0858047f06fa1e09afa
connected_slaves:1
slave0:ip=10.224.129.104,port=9971,conn_fd=104,lag=(db0:0)
db0:binlog_offset=2 384,safety_purge=none
`
	res, err := parseInfoReplication(text)
	if err != nil {
		fmt.Println(err)
		return
	}

	assert.Equal(t, res.DbBinlogFileNum, uint64(2), "db0 binlog file_num not right")
	assert.Equal(t, res.DbBinlogOffset, uint64(384), "db0 binlog offset not right")
	assert.Equal(t, len(res.Slaves), 1, "slaves numbers not right")
	assert.Equal(t, res.Slaves[0].IP, "10.224.129.104", "slave0 IP not right")
	assert.Equal(t, res.Slaves[0].Port, "9971", "slave0 Port not right")
}

func TestSlaveInfoReplication(t *testing.T) {
	text := `
# Replication(SLAVE)
role:slave
ReplicationID:94e8feeaf9036a77c59ad2f091f1c0b0858047f06fa1e09afa
master_host:10.224.129.40
master_port:9971
master_link_status:up
slave_priority:100
slave_read_only:1
db0:binlog_offset=1 284,safety_purge=none
`
	res, err := parseInfoReplication(text)
	if err != nil {
		fmt.Println(err)
		return
	}

	assert.Equal(t, res.DbBinlogFileNum, uint64(1), "db0 binlog file_num not right")
	assert.Equal(t, res.DbBinlogOffset, uint64(284), "db0 binlog offset not right")
	assert.Equal(t, len(res.Slaves), 0)
}
