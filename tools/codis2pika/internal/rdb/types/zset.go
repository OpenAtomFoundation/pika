package types

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

type ZSetEntry struct {
	Member string
	Score  string
	//Score int
}

type ZsetObject struct {
	Key      string
	Elements []ZSetEntry
}

func (o *ZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.Key = key
	switch typeByte {
	case rdbTypeZSet:
		o.readZset(rd)
	case rdbTypeZSet2:
		o.readZset2(rd)
	case rdbTypeZSetZiplist:
		o.readZsetZiplist(rd)
	case rdbTypeZSetListpack:
		o.readZsetListpack(rd)
	default:
		log.Panicf("unknown zset type. typeByte=[%d]", typeByte)
	}
}

type ZsetMember struct {
	Elements []ZSetEntry
}

type ZSetTempEntry struct {
	Member string
	//Score  string
	Score int
}

func (n *ZsetMember) UnmarshalJSON(text []byte) error {
	var readzset []ZSetEntry
	err := json.Unmarshal([]byte(string(text)), &readzset)

	for _, z := range readzset {
		newz := ZSetEntry{
			Member: z.Member,
			Score:  z.Score,
		}
		n.Elements = append(n.Elements, newz)
	}

	return err
}

func (o *ZsetObject) readZset(rd io.Reader) {
	var oline []ZSetEntry
	ele, err := io.ReadAll(rd)
	if err != nil {
		log.Panicf("readZset error : ", err, " when,", &oline)
	}
	err = json.Unmarshal(ele, &oline)
	if err != nil {
		log.Warnf("zset.go readZset: ", &oline, "   may err:", err)
	}

	o.Elements = oline
}

func (o *ZsetObject) readZset2(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	o.Elements = make([]ZSetEntry, size)
	for i := 0; i < size; i++ {
		o.Elements[i].Member = structure.ReadString(rd)
		score := structure.ReadDouble(rd)
		o.Elements[i].Score = fmt.Sprintf("%f", score)
	}
}

func (o *ZsetObject) readZsetZiplist(rd io.Reader) {
	list := structure.ReadZipList(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	o.Elements = make([]ZSetEntry, size/2)
	for i := 0; i < size; i += 2 {
		o.Elements[i/2].Member = list[i]
		o.Elements[i/2].Score = list[i+1]
	}
}

func (o *ZsetObject) readZsetListpack(rd io.Reader) {
	list := structure.ReadListpack(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	o.Elements = make([]ZSetEntry, size/2)
	for i := 0; i < size; i += 2 {
		o.Elements[i/2].Member = list[i]
		o.Elements[i/2].Score = list[i+1]
	}
}

func (o *ZsetObject) Rewrite() []RedisCmd {

	// var cmds []RedisCmd
	cmds := make([]RedisCmd, 0)
	if len(o.Elements) > 1 {
		var cmd RedisCmd
		cmd = append(cmd, "zadd", o.Key)

		for _, ele := range o.Elements {
			cmd = append(cmd, ele.Score, ele.Member)
		}
		cmds = append(cmds, cmd)
		return cmds
	} else {
		for _, ele := range o.Elements {
			cmd := RedisCmd{"zadd", o.Key, ele.Score, ele.Member}
			cmds = append(cmds, cmd)
		}
		return cmds
	}
}
