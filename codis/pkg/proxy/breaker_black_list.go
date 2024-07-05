package proxy

import (
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"strings"
	"sync"
)

type BreakerNameList struct {
	namelist sync.Map
}

var (
	cmdBlackListSwitch atomic2.Int64
	keyBlackListSwitch atomic2.Int64
	cmdBlackList = &BreakerNameList{}
	keyBlackList = &BreakerNameList{}
)


func XconfigGetListAsRedisResp(src string) *redis.Resp {
	var array []*redis.Resp = make([]*redis.Resp, 0)
	for _, i := range strings.Split(src, ",") {
		i = strings.TrimSpace(i)
		array = append(array, redis.NewBulkBytes([]byte(i)))
	}
	return redis.NewArray(array)
}

// cmd blacklist
func StoreCmdBlackList(cmdName string) {
	cmdName = strings.ToUpper(cmdName)
	cmdBlackList.namelist.Store(cmdName, struct{}{})
}
func StoreCmdBlackListByBatch(cmdList string) {
	if len(cmdList) <= 0 { //If the list is empty, set the switch to off
		cmdBlackListSwitch.Set(SWITCH_CLOSED)
	} else { //If the list is not empty, set the switch to On
		cmdBlackListSwitch.Set(SWITCH_OPEN)
	}
	for _, cmdName := range strings.Split(cmdList, ",") {
		cmdName = strings.TrimSpace(cmdName)
		StoreCmdBlackList(cmdName)
	}
}
func UpdateCmdBlackList(p *Proxy){
	var cmdlist []string
	cmdBlackList.namelist.Range(func(k, value any) bool {
		if k.(string)!="" {
		cmdlist = append(cmdlist, k.(string))
		}
		return true
	})
		p.config.BreakerCmdBlackList = strings.Join(cmdlist,",")
}
func AddCmdBlackList(p *Proxy, value string) {
	StoreCmdBlackList(value)
	UpdateCmdBlackList(p)
}
func CheckCmdBlackList(cmdName string) bool {
	cmdName = strings.ToUpper(cmdName)
	if _, ok := cmdBlackList.namelist.Load(cmdName); ok {
		return true
	}
	return false
}

// key blacklist
func StoreKeyBlackList(key string) {
	keyBlackList.namelist.Store(key, struct{}{})
}
func StoreKeyBlackLists(keyList string){
	for _, key := range strings.Split(keyList, ",") {
		// For the key of the blacklist, the non * part is extracted as the matching item
		key = strings.Trim(key, "*")
		key = strings.TrimSpace(key)
		StoreKeyBlackList(key)
	}
}
func StoreKeyBlackListByBatch(keyList string) {
	if len(keyList) <= 0 { //If the list is empty, set the switch to off
		keyBlackListSwitch.Set(SWITCH_CLOSED)
	} else { //If the list is not empty, set the switch to On
		keyBlackListSwitch.Set(SWITCH_OPEN)
	}
	StoreKeyBlackLists(keyList)
}
func UpdateKeyBlackList(p *Proxy){
	keylist :=make([]string,0)
	keyBlackList.namelist.Range(func(k, value any) bool {
		if k.(string)!="" {
		keylist = append(keylist, k.(string))
		}
		return true
	})
		p.config.BreakerKeyBlackList = strings.Join(keylist,",")
}
func AddKeyBlackList(p *Proxy, value string) {
	StoreKeyBlackList(value)
	UpdateKeyBlackList(p)
}

func CheckKeyBlackList(key string) bool {
	res := false
	// Perform a full query to check whether the entered key is in the map of the blacklist
	if _, ok := keyBlackList.namelist.Load(key); ok {
		return true
	}
	// The key in the whitelist may be part of the full name. For example, if the whitelist is set to social*, the key is stored as social in memory.
	// If the key to be checked is social-service, the condition is a match
	keyBlackList.namelist.Range(func(k, v interface{}) bool {
		if k != nil {
			if strings.HasPrefix(key, k.(string)) { // The prefix matching method is used temporarily, and the prefix matching efficiency is higher
				res = true
				return false
			}
		}
		return true
	})
	return res
}
