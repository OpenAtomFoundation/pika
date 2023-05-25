package types

import (
	"encoding/json"
	"io"
	"strconv"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

type SetObject struct {
	Key      string
	Elements []string
}

type SetMember struct {
	Elements []string
	numbers  []int
}

func (n *SetMember) UnmarshalJSON(text []byte) error {
	err := json.Unmarshal(text, &n.Elements)
	if err != nil {
		return json.Unmarshal(text, &n.numbers)
	}
	return err
}

func (o *SetObject) LoadFromBuffer(rd io.Reader, Key string, typeByte byte) {
	o.Key = Key
	switch typeByte {
	case rdbTypeSet:
		o.readSet(rd)
	case rdbTypeSetIntset:
		o.Elements = structure.ReadIntset(rd)
	default:
		log.Panicf("unknown set type. typeByte=[%d]", typeByte)
	}
}

func (o *SetObject) AddSet(setlist []string) {
	o.Elements = setlist
}

func (o *SetObject) readSet(rd io.Reader) {
	var oline SetMember
	ele, err := io.ReadAll(rd)
	if err != nil {
		log.Panicf("readSet error : ", err, " when,", &oline)
	}
	err = json.Unmarshal(ele, &oline)

	if err != nil {
		log.Warnf("set.go readSet: ", &oline, "   may err:", err)
	}
	o.Elements = append(o.Elements, oline.Elements...)
	for _, e := range oline.numbers {
		o.Elements = append(o.Elements, strconv.Itoa(e))
	}

}

func (o *SetObject) Rewrite() []RedisCmd {

	// var cmds []RedisCmd
	cmds := make([]RedisCmd, 0)
	if len(o.Elements) > 1 {
		var cmd RedisCmd
		cmd = append(cmd, "sadd", o.Key)

		for _, e := range o.Elements {
			cmd = append(cmd, e)
		}
		cmds = append(cmds, cmd)
		return cmds
	} else {
		for _, ele := range o.Elements {
			cmd := RedisCmd{"sadd", o.Key, ele}
			cmds = append(cmds, cmd)
		}
		return cmds
	}
}
