package types

import (
	"encoding/json"
	"io"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

type HashObject struct {
	key   string
	value map[string]string
}

func (o *HashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.value = make(map[string]string)
	switch typeByte {
	case rdbTypeHash:
		o.readHash(rd)
	case rdbTypeHashZipmap:
		o.readHashZipmap(rd)
	case rdbTypeHashZiplist:
		o.readHashZiplist(rd)
	case rdbTypeHashListpack:
		o.readHashListpack(rd)
	default:
		log.Panicf("unknown hash type. typeByte=[%d]", typeByte)
	}
}

type HashMember struct {
	elements map[string]string
}

func (n *HashMember) UnmarshalJSON(text []byte) error {
	stringtext := string(text)
	err := json.Unmarshal([]byte(stringtext), &n.elements)

	return err
}

func (o *HashObject) readHash(rd io.Reader) {
	var oline HashMember

	ele, err := io.ReadAll(rd)
	if err != nil {
		log.Panicf("readHash err: ", err, " when  ", &oline)
	}
	err = json.Unmarshal(ele, &oline)
	if err != nil {
		log.Warnf("hash.go readHash: ", &oline, "   may err:", err)
	}

	o.value = oline.elements
}

func (o *HashObject) readHashZipmap(rd io.Reader) {
	log.Panicf("not implemented rdbTypeZipmap")
}

func (o *HashObject) readHashZiplist(rd io.Reader) {
	list := structure.ReadZipList(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
}

func (o *HashObject) readHashListpack(rd io.Reader) {
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
}

func (o *HashObject) Rewrite() []RedisCmd {
	// var cmds []RedisCmd
	cmds := make([]RedisCmd, 0)
	if len(o.value) > 1 {
		var cmd RedisCmd
		cmd = append(cmd, "hmset", o.key)

		for k, v := range o.value {
			cmd = append(cmd, k, v)
		}
		cmds = append(cmds, cmd)
		return cmds
	} else {
		var cmds []RedisCmd
		for k, v := range o.value {
			cmd := RedisCmd{"hset", o.key, k, v}
			cmds = append(cmds, cmd)
		}
		return cmds
	}
}
