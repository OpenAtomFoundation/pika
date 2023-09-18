// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"hash/crc32"
	"strconv"
	"strings"

	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/errors"
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
type OpFlagMonitor uint32 //监控大key，大value的标志位

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

// 只要没有慢标志就认为是快命令
func (f OpFlag) IsQuick() bool {
	const mask = FlagSureSlow | FlagMaySlow
	return (f & mask) == 0
}

func (f OpFlag) IsSureQuick() bool {
	const mask = FlagSureQuick
	return (f & mask) != 0
}

func (f OpFlag) IsMayQuick() bool {
	const mask = FlagMayQuick
	return (f & mask) != 0
}

// 标志位：大key，大value判断
func (f OpFlagMonitor) NeedCheckBatchsizeOfRequest() bool {
	const mask = FlagReqKeys | FlagReqKeyFields
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckContentOfRequest() bool {
	const mask = FlagReqValues | FlagReqKeyValues | FlagReqKeyFieldValues | FlagReqKeyTtlValue
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckSingleValueOfResp() bool {
	const mask = FlagRespReturnSingleValue
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckNumberOfResp() bool {
	const mask = FlagRespReturnArraysize | FlagRespReturnValuesize
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckArrayOfResp() bool {
	const mask = FlagRespReturnArray | FlagRespReturnArrayByPair | FlagRespCheckArrayLength | FlagRespCheckArrayLengthByPair
	return (f & mask) != 0
}

func (f OpFlagMonitor) IsHighRisk() bool {
	const mask = FlagHighRisk
	return (f & mask) != 0
}

type CustomCheckFunc interface {
	CheckRequest(r *Request, s *Session) bool               //return true表示检查过了
	CheckResponse(r *Request, s *Session, delay int64) bool //return true表示检查过了
}

type OpInfo struct {
	Name string
	Flag OpFlag
}

var opTable = make(map[string]OpInfo, 256)

const (
	FlagWrite      = 1 << iota //1
	FlagMasterOnly             //2
	FlagMayWrite               //4
	FlagNotAllow               //8
	FlagSureQuick              //16
	FlagMayQuick               //32
	FlagSureSlow               //64
	FlagMaySlow                //128
)

const (
	// -- 请求部分
	// 1. 请求包含多个key或多个成员，没有值，即不需要考虑数据量的大小
	FlagReqKeys      = 1 << iota // 1     参数为：CMD Key1 ~ KeyN
	FlagReqKeyFields             // 2     参数为：CMD KEY Field1 ~ FieldN

	// 2. 请求不光包含key或成员，还操作了响应的值，需要检查数据量
	FlagReqValues         // 4     参数为：CMD KEY Value1 ~ ValueN
	FlagReqKeyValues      // 8     参数为：CMD (Key1,Value1) ~ (KeyN,ValueN)
	FlagReqKeyFieldValues // 16    参数为：CMD KEY (Field1,Value1) ~ (FieldN,ValueN)
	FlagReqKeyTtlValue    // 32    参数为：CMD KEY ttl value

	// -- 响应部分
	// 1. 响应返回的是单个数字，代表key的成员数量或者key本身大小（如果key是string类型），即对key的统计结果
	FlagRespReturnArraysize // 64    返回值是数组的长度
	FlagRespReturnValuesize //	128    返回值是string的长度

	// 2. 响应返回的是查询结果，代表key本身或者一部分
	FlagRespReturnSingleValue // 256   返回值是单个值
	FlagRespReturnArray       // 512   返回为数组，一个为一组
	FlagRespReturnArrayByPair // 1024   返回为数组，两个为一组

	// 3. 响应返回的是数组，但是只要检查数组大小，数组的内容不要检查
	FlagRespCheckArrayLength       // 2048  返回为数组，一个为一组, 只检查数组的长度，不检查数组内容
	FlagRespCheckArrayLengthByPair // 4096  返回为数组，两个为一组, 只检查数组的长度，不检查数组内容

	// -- 命令本身是高危操作，高危操作一定要被记录，即使不一定有风险
	FlagHighRisk // 8192  高风险命令
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
		{"EVAL", FlagWrite},
		{"EVALSHA", FlagWrite},
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
		{"PFMERGE", FlagWrite},
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
		{"RPOPLPUSH", FlagWrite},
		{"RPUSH", FlagWrite},
		{"RPUSHX", FlagWrite},
		{"SADD", FlagWrite},
		{"SAVE", FlagNotAllow},
		{"SCAN", FlagMasterOnly | FlagNotAllow},
		{"SCARD", 0},
		{"SCRIPT", FlagNotAllow},
		{"SDIFF", 0},
		{"SDIFFSTORE", FlagWrite},
		{"SELECT", 0},
		{"SET", FlagWrite},
		{"SETBIT", FlagWrite},
		{"SETEX", FlagWrite},
		{"SETNX", FlagWrite},
		{"SETRANGE", FlagWrite},
		{"SHUTDOWN", FlagNotAllow},
		{"SINTER", 0},
		{"SINTERSTORE", FlagWrite},
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
		{"SMOVE", FlagWrite},
		{"SORT", FlagWrite},
		{"SPOP", FlagWrite},
		{"SRANDMEMBER", 0},
		{"SREM", FlagWrite},
		{"SSCAN", FlagMasterOnly},
		{"STRLEN", 0},
		{"SUBSCRIBE", FlagNotAllow},
		{"SUBSTR", 0},
		{"SUNION", 0},
		{"SUNIONSTORE", FlagWrite},
		{"SYNC", FlagNotAllow},
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
		{"ZINTERSTORE", FlagWrite},
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
		{"ZUNIONSTORE", FlagWrite},
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
	var index = 0
	var bytes = 0

	for i := 0; i < len(multi); i++ {
		//cmd是固定大小切片，index最大等于cmd切片大小
		if index < len(cmd) {
			index += copy(cmd[index:], multi[i].Value)

			if i < len(multi)-1 {
				index += copy(cmd[index:], []byte(" "))
			}
		}

		bytes += len(multi[i].Value)

		//遍历所有元素后，如果cmd切片被填满则添加统计信息
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
