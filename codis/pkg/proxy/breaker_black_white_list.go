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
	cmdWhiteListSwitch atomic2.Int64
	keyWhiteListSwitch atomic2.Int64
	cmdBlackListSwitch atomic2.Int64
	keyBlackListSwitch atomic2.Int64

	cmdBlackList = &BreakerNameList{} //命令黑名单
	cmdWhiteList = &BreakerNameList{} //命令白名单
	keyBlackList = &BreakerNameList{} //key黑名单
	keyWhiteList = &BreakerNameList{} //key白名单
)

func (nl *BreakerNameList) clear() {
	nl.namelist.Range(func(key, value interface{}) bool {
		nl.namelist.Delete(key)
		return true
	})
}

func XconfigGetListAsRedisResp(src string) *redis.Resp {
	var array []*redis.Resp = make([]*redis.Resp, 0)
	for _, i := range strings.Split(src, ",") {
		i = strings.TrimSpace(i)
		array = append(array, redis.NewBulkBytes([]byte(i)))
	}
	return redis.NewArray(array)
}

// 命令黑名单
func StoreCmdBlackList(cmdName string) {
	cmdName = strings.ToUpper(cmdName) //强制转换成大写的命令
	cmdBlackList.namelist.Store(cmdName, struct{}{})
}
func StoreCmdBlackListByBatch(cmdList string) {
	// 由于xconfig命令给的是全量覆盖，所以每次修改均需对原map内容进行一次清空
	cmdBlackList.clear()
	if len(cmdList) <= 0 { //说明名单为空，将开关置为关闭状态
		cmdBlackListSwitch.Set(SWITCH_CLOSED)
	} else { //名单不为空，将开关置为开启状态
		cmdBlackListSwitch.Set(SWITCH_OPEN)
	}
	for _, cmdName := range strings.Split(cmdList, ",") {
		cmdName = strings.TrimSpace(cmdName)
		StoreCmdBlackList(cmdName)
	}
}
func CheckCmdBlackList(cmdName string) bool {
	cmdName = strings.ToUpper(cmdName) //强制转换成大写的命令
	if _, ok := cmdBlackList.namelist.Load(cmdName); ok {
		return true
	}
	return false
}

// 命令白名单
func StoreCmdWhiteList(cmdName string) {
	cmdName = strings.ToUpper(cmdName) //强制转换成大写的命令
	cmdWhiteList.namelist.Store(cmdName, struct{}{})
}
func StoreCmdWhiteListByBatch(cmdList string) {
	cmdWhiteList.clear()
	if len(cmdList) <= 0 { //说明名单为空，将开关置为关闭状态
		cmdWhiteListSwitch.Set(SWITCH_CLOSED)
	} else { //名单不为空，将开关置为开启状态
		cmdWhiteListSwitch.Set(SWITCH_OPEN)
	}
	for _, cmdName := range strings.Split(cmdList, ",") {
		cmdName = strings.TrimSpace(cmdName)
		StoreCmdWhiteList(cmdName)
	}
}
func CheckCmdWhiteList(cmdName string) bool {
	cmdName = strings.ToUpper(cmdName) //强制转换成大写的命令
	if _, ok := cmdWhiteList.namelist.Load(cmdName); ok {
		return true
	}
	return false
}

// key黑名单
func StoreKeyBlackList(key string) {
	keyBlackList.namelist.Store(key, struct{}{})
}

func StoreKeyBlackListByBatch(keyList string) {
	keyBlackList.clear()
	if len(keyList) <= 0 { //说明名单为空，将开关置为关闭状态
		keyBlackListSwitch.Set(SWITCH_CLOSED)
	} else { //名单不为空，将开关置为开启状态
		keyBlackListSwitch.Set(SWITCH_OPEN)
	}
	for _, key := range strings.Split(keyList, ",") {
		// 对黑白名单的key，提取非*部分作为匹配项
		key = strings.Trim(key, "*")
		key = strings.TrimSpace(key)
		StoreKeyBlackList(key)
	}
}

func CheckKeyBlackList(key string) bool {
	res := false
	// 先进行全量查询，查询输入的key是否在黑名单的map中
	if _, ok := keyBlackList.namelist.Load(key); ok {
		return true
	}
	// 黑白名单中的key可能为全名的一部分，比如，黑白名单设置为social*，在内存中该key存储为social，
	// 而待检查的key为social-service，此情况属于匹配的
	keyBlackList.namelist.Range(func(k, v interface{}) bool {
		if k != nil {
			if strings.HasPrefix(key, k.(string)) { // 暂时使用前缀匹配，前缀匹配效率较高
				res = true
				return false // 退出sync.Map的迭代
			}
		}
		return true // 继续sync.Map的迭代
	})
	return res
}

// key白名单
func StoreKeyWhiteList(key string) {
	keyWhiteList.namelist.Store(key, struct{}{})
}
func StoreKeyWhiteListByBatch(keyList string) {
	keyWhiteList.clear()
	if len(keyList) <= 0 { //说明名单为空，将开关置为关闭状态
		keyWhiteListSwitch.Set(SWITCH_CLOSED)
	} else { //名单不为空，将开关置为开启状态
		keyWhiteListSwitch.Set(SWITCH_OPEN)
	}
	for _, key := range strings.Split(keyList, ",") {
		// 对黑白名单的key，提取非*部分作为匹配项
		key = strings.Trim(key, "*")
		key = strings.TrimSpace(key)
		StoreKeyWhiteList(key)
	}
}

func CheckKeyWhiteList(key string) bool {
	res := false
	// 先进行全量查询，查询输入的key是否在白名单的map中
	if _, ok := keyWhiteList.namelist.Load(key); ok {
		return true
	}
	// 黑白名单中的key可能为全名的一部分，比如，黑白名单设置为social*，在内存中该key存储为social，
	// 而待检查的key为social-service，此情况属于匹配的
	keyWhiteList.namelist.Range(func(k, v interface{}) bool {
		if k != nil {
			if strings.HasPrefix(key, k.(string)) { // 暂时使用前缀匹配，前缀匹配效率较高
				res = true
				return false // 退出sync.Map的迭代
			}
		}
		return true // 继续sync.Map的迭代
	})
	return res
}
