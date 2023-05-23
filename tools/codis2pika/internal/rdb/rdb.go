package rdb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/config"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/types"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/statistics"

	// "github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/parser"
)

type Loader struct {
	replStreamDbId int
	nowDBId        int
	expireAt       uint64
	idle           int64
	freq           int64

	filPath string
	fp      *os.File

	ch chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

// {"db":0,"key":"string","size":10,"type":"string","value":"aaaaaaa"},
type KeyReadFromJson struct {
	Bb      int     `json:"db"`
	Key     string  `json:"key"`
	Size    uint64  `json:"size"`
	Keytype string  `json:"type"`
	Value   linectx `json:"value"`
	Values  linectx `json:"values"`
	Members linectx `json:"members"`
	Hash    linectx `json:"hash"`
	Entries linectx `json:"entries"`
	Other   interface{}
}

// 解析rdb文件，此处zset暂时默认db=0
func (ld *Loader) NewParseRDB() int {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. file_path=[%s], error=[%s]", ld.filPath, err)
		}
	}()
	rd := bufio.NewReader(ld.fp)

	//magic + version
	buf := make([]byte, 9)
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		log.PanicError(err)
	}
	if !bytes.Equal(buf[:5], []byte("REDIS")) {
		log.Panicf("verify magic string, invalid file format. bytes=[%v]", buf[:5])
	}
	version, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("RDB version: %d", version)

	rdbFile, err := os.Open(ld.filPath)
	if err != nil {
		panic("open dump.rdb failed")
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	// for stat
	UpdateRDBSentSize := func() {
		offset, err := rdbFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.PanicError(err)
		}
		statistics.UpdateRDBSentSize(offset)
	}
	defer UpdateRDBSentSize()

	decoder := parser.NewDecoder(rdbFile)
	log.Infof("start parseRDBEntry")

	err = decoder.Parse(func(o parser.RedisObject) bool {

		var lines []byte
		var setlines []string
		var jline KeyReadFromJson

		typeByte := o.GetType()
		expireAt := o.GetExpiration()
		if expireAt != nil {
			nowtime := time.Now()
			//对已经过期或快过期的key，设置过期缓存3s
			if !nowtime.Before(expireAt.Add(-3 * time.Second)) {
				ld.expireAt = uint64(6)
			}
			if nowtime.Equal(expireAt.Add(-3 * time.Second)) {
				ld.expireAt = uint64(6)
			}
			ld.expireAt = uint64(expireAt.Sub(nowtime).Seconds())
			//对于超时计算异常的key，过期时间往往特别巨大，此时特殊处理保留一段时间
			if ld.expireAt > uint64(99999999) {
				log.Infof("Anomalous ttl of key: ", o.GetKey(), " ", ld.expireAt)
				ld.expireAt = uint64(1000000)
			}

		}
		jsize := o.GetSize()

		jkey := o.GetKey()
		if uint64(jsize) > config.Config.Advanced.TargetRedisProtoMaxBulkLen {
			fmt.Println("bigkey not supported")
		}

		keytype := GetStringToBytes(typeByte)

		switch typeByte {
		case parser.StringType:
			str := o.(*parser.StringObject)
			lines = str.Value
			o := types.ParseObject(bytes.NewReader(lines), keytype, o.GetKey())
			cmds := o.Rewrite()
			for _, cmd := range cmds {
				ld.cmd2channel(cmd)
			}
		case parser.SetType:
			set := o.(*parser.SetObject)
			for _, v := range set.Members {
				setlines = append(setlines, string(v))
			}
			var newo = new(types.SetObject)
			newo.Elements = setlines
			newo.Key = jkey
			newo.AddSet(setlines)

			cmds := newo.Rewrite()
			for _, cmd := range cmds {
				ld.cmd2channel(cmd)
			}
		case parser.ListType:
			list := o.(*parser.ListObject)
			for _, v := range list.Values {
				setlines = append(setlines, string(v))
			}
			var newo = new(types.ListObject)
			newo.Elements = setlines
			newo.Key = jkey
			newo.AddList(setlines)

			cmds := newo.Rewrite()
			for _, cmd := range cmds {

				ld.cmd2channel(cmd)
			}
		case parser.HashType:
			hash := o.(*parser.HashObject)
			lines, err = hash.MarshalJSON()
			err = json.Unmarshal(lines, &jline)
			if err != nil {
				log.Warnf("ParseRDB get unmarshal hash values error :", err.Error())
			}
			keyvalue := GetValue(jline, typeByte).string()
			anotherReader := io.Reader(bytes.NewBufferString(keyvalue))
			o := types.ParseObject(anotherReader, keytype, jkey)
			cmds := o.Rewrite()
			for _, cmd := range cmds {
				ld.cmd2channel(cmd)
			}
		case parser.ZSetType:
			zset := o.(*parser.ZSetObject)
			var n []types.ZSetEntry
			for _, zentry := range zset.Entries {
				newz := types.ZSetEntry{
					Member: zentry.Member,
					Score:  strconv.Itoa(int(zentry.Score)),
				}
				n = append(n, newz)
			}
			var newo = new(types.ZsetObject)
			newo.Key = o.GetKey()
			newo.Elements = n
			cmds := newo.Rewrite()
			for _, cmd := range cmds {
				ld.cmd2channel(cmd)
			}
		}

		if expireAt != nil {
			var expirycmd types.RedisCmd
			expirycmd = append(expirycmd, "EXPIRE", jkey, fmt.Sprint(ld.expireAt))
			ld.cmd2channel(expirycmd)
		}

		ld.expireAt = 0
		ld.idle = 0
		ld.freq = 0

		// return true to continue, return false to stop the iteration
		return true
	})
	if err != nil {
		panic(err)
	}
	log.Infof("finish parseRDBEntry")
	return ld.replStreamDbId
}

func (ld *Loader) cmd2channel(cmd types.RedisCmd) {
	e := entry.NewEntry()
	e.IsBase = true
	e.DbId = ld.nowDBId
	if cmd == nil {
		log.Warnf("the cmd is nil.")
	} else {
		e.Argv = cmd
		ld.ch <- e
	}

}

type TmpZSetEntry struct {
	Member string
	Score  int
}

type linectx struct {
	cplx []string
	smpl string
	hash map[string]string
	zset []types.ZSetEntry
}

func (n *linectx) UnmarshalJSON(text []byte) error {
	var readzset []types.ZSetEntry
	var tmpzset []types.ZSetEntry
	t := strings.TrimSpace(string(text))
	if strings.HasPrefix(t, "[") {

		err := json.Unmarshal(text, &n.cplx)
		if err != nil {

			err = json.Unmarshal(text, &n.hash)
			if err != nil {

				err := json.Unmarshal(text, &tmpzset)

				for _, z := range tmpzset {
					newz := types.ZSetEntry{
						Member: z.Member,
						Score:  z.Score,
					}
					readzset = append(readzset, newz)
				}
				n.zset = readzset

				return err
			}
		}
	}

	err := json.Unmarshal(text, &n.smpl)
	if err != nil {

		err = json.Unmarshal(text, &n.hash)
		if err != nil {

			err := json.Unmarshal(text, &tmpzset)

			for _, z := range tmpzset {
				newz := types.ZSetEntry{
					Member: z.Member,
					Score:  z.Score,
				}
				readzset = append(readzset, newz)
			}
			n.zset = readzset

			return err
		}
	}
	return err
}

// 根据key的类型，返回执行的set命令
func SetCommandofThis(keytype string) string {
	switch keytype {
	case types.StringType:
		return "SET"
	case types.HashType:
		return "HSET"
	case types.ListType:
		return "LSET"
	case types.ZSetType:
		return "ZADD"
	case types.SetType:
		return "SADD"
	}
	log.Panicf("unknown type byte: %s", keytype)
	return "SET"
}

const (
	rdbTypeString  = 0 // RDB_TYPE_STRING
	rdbTypeList    = 1
	rdbTypeSet     = 2
	rdbTypeZSet    = 3
	rdbTypeHash    = 4 // RDB_TYPE_HASH
	rdbTypeZSet2   = 5 // ZSET version 2 with doubles stored in binary.
	rdbTypeModule  = 6 // RDB_TYPE_MODULE
	rdbTypeModule2 = 7 // RDB_TYPE_MODULE2 Module value with annotations for parsing without the generating module being loaded.

	// Object types for encoded objects.

	rdbTypeHashZipmap       = 9
	rdbTypeListZiplist      = 10
	rdbTypeSetIntset        = 11
	rdbTypeZSetZiplist      = 12
	rdbTypeHashZiplist      = 13
	rdbTypeListQuicklist    = 14 // RDB_TYPE_LIST_QUICKLIST
	rdbTypeStreamListpacks  = 15 // RDB_TYPE_STREAM_LISTPACKS
	rdbTypeHashListpack     = 16 // RDB_TYPE_HASH_ZIPLIST
	rdbTypeZSetListpack     = 17 // RDB_TYPE_ZSET_LISTPACK
	rdbTypeListQuicklist2   = 18 // RDB_TYPE_LIST_QUICKLIST_2 https://github.com/redis/redis/pull/9357
	rdbTypeStreamListpacks2 = 19 // RDB_TYPE_STREAM_LISTPACKS2

	moduleTypeNameCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

// 根据类型与原始的value string，解析对象，返回值
func ParseJsonObject(rd io.Reader, typeByte byte, key string) interface{} {
	switch typeByte {
	case rdbTypeString: // string
		o := new(types.StringObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeList, rdbTypeListZiplist, rdbTypeListQuicklist, rdbTypeListQuicklist2: // list
		o := new(types.ListObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeSet, rdbTypeSetIntset: // set
		o := new(types.SetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeZSet, rdbTypeZSet2, rdbTypeZSetZiplist, rdbTypeZSetListpack: // zset
		o := new(types.ZsetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeHash, rdbTypeHashZipmap, rdbTypeHashZiplist, rdbTypeHashListpack: // hash
		o := new(types.HashObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeStreamListpacks, rdbTypeStreamListpacks2: // stream
		o := new(types.StreamObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeModule, rdbTypeModule2: // module
		if typeByte == rdbTypeModule {
			log.Panicf("module type is not supported")
		}
		moduleId := structure.ReadLength(rd)
		moduleName := moduleTypeNameByID(moduleId)
		switch moduleName {
		case "exhash---":
			log.Panicf("exhash module is not supported")
		case "exstrtype":
			log.Panicf("exstrtype module is not supported")
		case "tair-json":
			log.Panicf("tair-json module is not supported")
		default:
			log.Panicf("unknown module type: %s", moduleName)
		}
	}
	log.Panicf("unknown type byte: %d", typeByte)
	return nil
}

func moduleTypeNameByID(moduleId uint64) string {
	nameList := make([]byte, 9)
	moduleId >>= 10
	for i := 8; i >= 0; i-- {
		nameList[i] = moduleTypeNameCharSet[moduleId&63]
		moduleId >>= 6
	}
	return string(nameList)
}

func StringToBytes(data string) byte {
	return *(*byte)(unsafe.Pointer(&data))
}

func StringToBytes2(data string) []byte {
	return *(*[]byte)(unsafe.Pointer(&data))
}

// 用于适配redis shake的解析方法
func GetStringToBytes(data string) byte {
	switch data {
	case types.StringType:
		return rdbTypeString
	case types.HashType:
		return rdbTypeHash
	case types.ListType:
		return rdbTypeList
	case types.ZSetType:
		return rdbTypeZSet
	case types.SetType:
		return rdbTypeSet
	}
	log.Panicf("unknown type byte: %s,try to set as rdbTypeString", data)

	return rdbTypeString
}

// 适配不同类型的key，获取值的方式不同
func GetValue(jline KeyReadFromJson, keytype string) linectx {
	switch keytype {
	case types.StringType:
		return jline.Value
	case types.HashType:
		jline.Value = jline.Hash
	case types.ListType:
		jline.Value = jline.Values
	case types.ZSetType:
		jline.Value = jline.Entries
	case types.SetType:
		jline.Value = jline.Members
	}
	return jline.Value
}

// 将linectx转为string
func (n linectx) string() string {

	if len(n.hash) != 0 {
		newb, err := json.Marshal(n.hash)

		if err != nil {
			return "linectx structure hash transfor to string error"
		}

		return string(newb)
	}

	if NotNil(n.cplx) {
		return func(nlist []string) string {
			// for _, v := range nlist {
			// 	newv = newv + " " + v
			// }
			newb, err := json.Marshal(n.cplx)
			if err != nil {
				return err.Error()
			}
			return string(newb)
		}(n.cplx)
	}
	if n.smpl != "" {
		return n.smpl
	}

	if len(n.zset) != 0 {

		newb, err := json.Marshal(n.zset)
		if err != nil {
			return "linectx structure zset transfor to string error"
		}

		return string(newb)
	}

	return "[error] linectx structure transfor to string error"
}

func NotNil(ctx []string) bool {
	var ctxstring string
	for _, v := range ctx {
		ctxstring = ctxstring + v
	}
	return len(ctxstring) > 0
}
