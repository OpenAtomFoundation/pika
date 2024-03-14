// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"

	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
)

var charmap [256]byte

func init() {
	for i := range charmap {
		c := byte(i)
		switch {
		case c >= 'A' && c <= 'Z':
			charmap[i] = c
		case c >= 'a' && c <= 'z':
			charmap[i] = c - 'a' + 'A'
		case c == ':':
			charmap[i] = ':'
		}
	}
}

type OpFlag uint32

func (f OpFlag) IsNotAllowed() bool {
	return (f & FlagNotAllow) != 0
}

func (f OpFlag) IsReadOnly() bool {
	const mask = FlagWrite | FlagMayWrite
	return (f & mask) == 0
}

func (f OpFlag) IsMasterOnly() bool {
	const mask = FlagWrite | FlagMayWrite | FlagMasterOnly
	return (f & mask) != 0
}

func (f OpFlag) IsQuick() bool {
	return (f & FlagQuick) != 0
}

type OpInfo struct {
	Name string
	Flag OpFlag
}

const (
	FlagWrite OpFlag = 1 << iota
	FlagMasterOnly
	FlagMayWrite
	FlagNotAllow
	FlagQuick
	FlagSlow
)

var (
	opTableLock sync.RWMutex
	opTable     = make(map[string]OpInfo, 256)
)

func init() {
	for _, i := range []OpInfo{
		{"APPEND", FlagWrite},
		{"ASKING", FlagNotAllow},
		{"AUTH", 0},
		{"BGREWRITEAOF", FlagNotAllow},
		{"BGSAVE", FlagNotAllow},
		{"BITCOUNT", 0},
		{"BITFIELD", FlagWrite},
		{"BITOP", FlagWrite | FlagNotAllow},
		{"BITPOS", 0},
		{"BLPOP", FlagWrite | FlagNotAllow},
		{"BRPOP", FlagWrite | FlagNotAllow},
		{"BRPOPLPUSH", FlagWrite | FlagNotAllow},
		{"CLIENT", FlagNotAllow},
		{"CLUSTER", FlagNotAllow},
		{"COMMAND", 0},
		{"CONFIG", FlagNotAllow},
		{"DBSIZE", FlagNotAllow},
		{"DEBUG", FlagNotAllow},
		{"DECR", FlagWrite},
		{"DECRBY", FlagWrite},
		{"DEL", FlagWrite},
		{"DISCARD", FlagNotAllow},
		{"DUMP", 0},
		{"ECHO", 0},
		{"EVAL", FlagNotAllow},
		{"EVALSHA", FlagNotAllow},
		{"EXEC", FlagNotAllow},
		{"EXISTS", 0},
		{"EXPIRE", FlagWrite},
		{"EXPIREAT", FlagWrite},
		{"FLUSHALL", FlagWrite | FlagNotAllow},
		{"FLUSHDB", FlagWrite | FlagNotAllow},
		{"GEOADD", FlagWrite},
		{"GEODIST", 0},
		{"GEOHASH", 0},
		{"GEOPOS", 0},
		{"GEORADIUS", FlagWrite},
		{"GEORADIUSBYMEMBER", FlagWrite},
		{"GET", 0},
		{"GETBIT", 0},
		{"GETRANGE", 0},
		{"GETSET", FlagWrite},
		{"HDEL", FlagWrite},
		{"HEXISTS", 0},
		{"HGET", 0},
		{"HGETALL", 0},
		{"HINCRBY", FlagWrite},
		{"HINCRBYFLOAT", FlagWrite},
		{"HKEYS", 0},
		{"HLEN", 0},
		{"HMGET", 0},
		{"HMSET", FlagWrite},
		{"HOST:", FlagNotAllow},
		{"HSCAN", FlagMasterOnly},
		{"HSET", FlagWrite},
		{"HSETNX", FlagWrite},
		{"HSTRLEN", 0},
		{"HVALS", 0},
		{"INCR", FlagWrite},
		{"INCRBY", FlagWrite},
		{"INCRBYFLOAT", FlagWrite},
		{"INFO", 0},
		{"KEYS", FlagNotAllow},
		{"LASTSAVE", FlagNotAllow},
		{"LATENCY", FlagNotAllow},
		{"LINDEX", 0},
		{"LINSERT", FlagWrite},
		{"LLEN", 0},
		{"LPOP", FlagWrite},
		{"LPUSH", FlagWrite},
		{"LPUSHX", FlagWrite},
		{"LRANGE", 0},
		{"LREM", FlagWrite},
		{"LSET", FlagWrite},
		{"LTRIM", FlagWrite},
		{"MGET", 0},
		{"MIGRATE", FlagWrite | FlagNotAllow},
		{"MONITOR", FlagNotAllow},
		{"MOVE", FlagWrite | FlagNotAllow},
		{"MSET", FlagWrite},
		{"MSETNX", FlagWrite | FlagNotAllow},
		{"MULTI", FlagNotAllow},
		{"OBJECT", FlagNotAllow},
		{"PERSIST", FlagWrite},
		{"PEXPIRE", FlagWrite},
		{"PEXPIREAT", FlagWrite},
		{"PFADD", FlagWrite},
		{"PFCOUNT", 0},
		{"PFDEBUG", FlagWrite},
		{"PFMERGE", FlagNotAllow},
		{"PFSELFTEST", 0},
		{"PING", 0},
		{"POST", FlagNotAllow},
		{"PSETEX", FlagWrite},
		{"PSUBSCRIBE", FlagNotAllow},
		{"PSYNC", FlagNotAllow},
		{"PTTL", 0},
		{"PUBLISH", FlagNotAllow},
		{"PUBSUB", 0},
		{"PUNSUBSCRIBE", FlagNotAllow},
		{"QUIT", 0},
		{"RANDOMKEY", FlagNotAllow},
		{"READONLY", FlagNotAllow},
		{"READWRITE", FlagNotAllow},
		{"RENAME", FlagWrite | FlagNotAllow},
		{"RENAMENX", FlagWrite | FlagNotAllow},
		{"REPLCONF", FlagNotAllow},
		{"RESTORE", FlagWrite | FlagNotAllow},
		{"RESTORE-ASKING", FlagWrite | FlagNotAllow},
		{"ROLE", 0},
		{"RPOP", FlagWrite},
		{"RPOPLPUSH", FlagNotAllow},
		{"RPUSH", FlagWrite},
		{"RPUSHX", FlagWrite},
		{"SADD", FlagWrite},
		{"SAVE", FlagNotAllow},
		{"SCAN", FlagMasterOnly | FlagNotAllow},
		{"SCARD", 0},
		{"SCRIPT", FlagNotAllow},
		{"SDIFF", FlagNotAllow},
		{"SDIFFSTORE", FlagWrite},
		{"SELECT", 0},
		{"SET", FlagWrite},
		{"SETBIT", FlagWrite},
		{"SETEX", FlagWrite},
		{"SETNX", FlagWrite},
		{"SETRANGE", FlagWrite},
		{"SHUTDOWN", FlagNotAllow},
		{"SINTER", FlagNotAllow},
		{"SINTERSTORE", FlagNotAllow},
		{"SISMEMBER", 0},
		{"SLAVEOF", FlagNotAllow},
		{"SLOTSCHECK", FlagNotAllow},
		{"SLOTSDEL", FlagWrite | FlagNotAllow},
		{"SLOTSHASHKEY", 0},
		{"SLOTSINFO", FlagMasterOnly},
		{"SLOTSMAPPING", 0},
		{"SLOTSMGRTONE", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTSLOT", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTTAGONE", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTTAGSLOT", FlagWrite | FlagNotAllow},
		{"SLOTSRESTORE", FlagWrite},
		{"SLOTSMGRTONE-ASYNC", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTSLOT-ASYNC", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTTAGONE-ASYNC", FlagWrite | FlagNotAllow},
		{"SLOTSMGRTTAGSLOT-ASYNC", FlagWrite | FlagNotAllow},
		{"SLOTSMGRT-ASYNC-FENCE", FlagNotAllow},
		{"SLOTSMGRT-ASYNC-CANCEL", FlagNotAllow},
		{"SLOTSMGRT-ASYNC-STATUS", FlagNotAllow},
		{"SLOTSMGRT-EXEC-WRAPPER", FlagWrite | FlagNotAllow},
		{"SLOTSRESTORE-ASYNC", FlagWrite | FlagNotAllow},
		{"SLOTSRESTORE-ASYNC-AUTH", FlagWrite | FlagNotAllow},
		{"SLOTSRESTORE-ASYNC-ACK", FlagWrite | FlagNotAllow},
		{"SLOTSSCAN", FlagMasterOnly},
		{"SLOWLOG", FlagNotAllow},
		{"SMEMBERS", 0},
		{"SMOVE", FlagNotAllow},
		{"SORT", FlagWrite},
		{"SPOP", FlagWrite},
		{"SRANDMEMBER", 0},
		{"SREM", FlagWrite},
		{"SSCAN", FlagMasterOnly},
		{"STRLEN", 0},
		{"SUBSCRIBE", FlagNotAllow},
		{"SUBSTR", 0},
		{"SUNION", FlagNotAllow},
		{"SUNIONSTORE", FlagNotAllow},
		{"SYNC", FlagNotAllow},
		{"PCONFIG", 0},
		{"TIME", FlagNotAllow},
		{"TOUCH", FlagWrite},
		{"TTL", 0},
		{"TYPE", 0},
		{"UNSUBSCRIBE", FlagNotAllow},
		{"UNWATCH", FlagNotAllow},
		{"WAIT", FlagNotAllow},
		{"WATCH", FlagNotAllow},
		{"ZADD", FlagWrite},
		{"ZCARD", 0},
		{"ZCOUNT", 0},
		{"ZINCRBY", FlagWrite},
		{"ZINTERSTORE", FlagNotAllow},
		{"ZLEXCOUNT", 0},
		{"ZRANGE", 0},
		{"ZRANGEBYLEX", 0},
		{"ZRANGEBYSCORE", 0},
		{"ZRANK", 0},
		{"ZREM", FlagWrite},
		{"ZREMRANGEBYLEX", FlagWrite},
		{"ZREMRANGEBYRANK", FlagWrite},
		{"ZREMRANGEBYSCORE", FlagWrite},
		{"ZREVRANGE", 0},
		{"ZREVRANGEBYLEX", 0},
		{"ZREVRANGEBYSCORE", 0},
		{"ZREVRANK", 0},
		{"ZSCAN", FlagMasterOnly},
		{"ZSCORE", 0},
		{"ZUNIONSTORE", FlagNotAllow},
	} {
		opTable[i.Name] = i
	}
}

var (
	ErrBadMultiBulk = errors.New("bad multi-bulk for command")
	ErrBadOpStrLen  = errors.New("bad command length, too short or too long")
)

const MaxOpStrLen = 64

func getOpInfo(multi []*redis.Resp) (string, OpFlag, error) {
	if len(multi) < 1 {
		return "", 0, ErrBadMultiBulk
	}

	var upper [MaxOpStrLen]byte

	var op = multi[0].Value
	if len(op) == 0 || len(op) > len(upper) {
		return "", 0, ErrBadOpStrLen
	}
	for i := range op {
		if c := charmap[op[i]]; c != 0 {
			upper[i] = c
		} else {
			return strings.ToUpper(string(op)), FlagMayWrite, nil
		}
	}
	op = upper[:len(op)]

	opTableLock.RLock()
	defer opTableLock.RUnlock()

	if r, ok := opTable[string(op)]; ok {
		return r.Name, r.Flag, nil
	}
	return string(op), FlagMayWrite, nil
}

func Hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}

func getHashKey(multi []*redis.Resp, opstr string) []byte {
	var index = 1
	switch opstr {
	case "ZINTERSTORE", "ZUNIONSTORE", "EVAL", "EVALSHA":
		index = 3
	}
	if index < len(multi) {
		return multi[index].Value
	}
	return nil
}

func getWholeCmd(multi []*redis.Resp, cmd []byte) int {
	var (
		index = 0
		bytes = 0
	)
	for i := 0; i < len(multi); i++ {
		if index < len(cmd) {
			index += copy(cmd[index:], multi[i].Value)
			if i < len(multi)-i {
				index += copy(cmd[index:], []byte(" "))
			}
		}
		bytes += len(multi[i].Value)

		if i == len(multi)-1 && index == len(cmd) {
			more := []byte("... " + strconv.Itoa(len(multi)) + " elements " + strconv.Itoa(bytes) + " bytes.")
			index = len(cmd) - len(more)
			if index < 0 {
				index = 0
			}
			index += copy(cmd[index:], more)
			break
		}
	}
	return index
}

func setCmdListFlag(cmdlist string, flag OpFlag) error {
	reverseFlag := FlagSlow
	flagString := "FlagQuick"
	if flag&FlagSlow != 0 {
		reverseFlag = FlagQuick
		flagString = "FlagSlow"
	}

	opTableLock.Lock()
	defer opTableLock.Unlock()

	for _, r := range opTable {
		r.Flag = r.Flag &^ flag
		opTable[r.Name] = r
	}
	if len(cmdlist) == 0 {
		return nil
	}
	cmdlist = strings.ToUpper(cmdlist)
	cmds := strings.Split(cmdlist, ",")
	for i := 0; i < len(cmds); i++ {
		if r, ok := opTable[strings.TrimSpace(cmds[i])]; ok {
			log.Infof("before setCmdListFlag: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			if r.Flag&reverseFlag == 0 {
				r.Flag = r.Flag | flag
				opTable[strings.TrimSpace(cmds[i])] = r
				log.Infof("after setCmdListFlag: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			} else {
				log.Warnf("cmd[%s] is %s command.", cmds[i], flagString)
				return errors.Errorf("cmd[%s] is %s command.", cmds[i], flagString)
			}
		} else {
			log.Warnf("can not find [%s] command.", cmds[i])
			return errors.Errorf("can not find [%s] command.", cmds[i])
		}
	}
	return nil
}

func getCmdFlag() *redis.Resp {
	var array = make([]*redis.Resp, 0, 32)
	const mask = FlagQuick | FlagSlow

	opTableLock.RLock()
	defer opTableLock.RUnlock()

	for _, r := range opTable {
		if r.Flag&mask != 0 {
			retStr := r.Name + " : Flag[" + strconv.Itoa(int(r.Flag)) + "]"

			if r.Flag&FlagQuick != 0 {
				retStr += ", FlagQuick"
			}

			if r.Flag&FlagSlow != 0 {
				retStr += ", FlagSlow"
			}

			array = append(array, redis.NewBulkBytes([]byte(retStr)))
		}
	}
	return redis.NewArray(array)
}
