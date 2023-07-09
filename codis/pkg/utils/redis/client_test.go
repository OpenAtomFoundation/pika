package redis

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestKk(t *testing.T) {
	ok, err := regexp.Match("slave[0-9]+", []byte("slave_01"))

	fmt.Sprintln(ok, err)
}

func TestParseInfo(t *testing.T) {
	text := "# Replication\nrole:master\nconnected_slaves:1\nslave0:ip=10.174.22.228,port=9225,state=online,offset=2175592,lag=0\nmaster_repl_offset:2175592\nrepl_backlog_active:1\nrepl_backlog_size:1048576\nrepl_backlog_first_byte_offset:1127017\nrepl_backlog_histlen:1048576\n"
	info := make(map[string]string)
	slaveMap := make([]map[string]string, 0)
	var slaves []InfoSlave
	var infoReplication InfoReplication

	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}

		if key := strings.TrimSpace(kv[0]); key != "" {
			if ok, _ := regexp.Match("slave[0-9]+", []byte(key)); ok {
				slaveKvs := strings.Split(kv[1], ",")

				slave := make(map[string]string)
				for _, slaveKvStr := range slaveKvs {
					slaveKv := strings.Split(slaveKvStr, "=")
					if len(slaveKv) != 2 {
						continue
					}
					slave[slaveKv[0]] = slaveKv[1]
				}

				slaveMap = append(slaveMap, slave)
			} else {
				info[key] = strings.TrimSpace(kv[1])
			}
		}
	}
	if len(slaveMap) > 0 {
		slavesStr, _ := json.Marshal(slaveMap)
		err := json.Unmarshal(slavesStr, &slaves)

		_ = err
		info["slaveMap"] = string(slavesStr)
	}

	str, _ := json.Marshal(info)
	err := json.Unmarshal(str, &infoReplication)
	infoReplication.Slaves = slaves

	_ = err
	fmt.Println(err)
}
