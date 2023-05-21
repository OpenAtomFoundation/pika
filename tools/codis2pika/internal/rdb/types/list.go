package types

import (
	"encoding/json"
	"io"
	"strconv"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

// quicklist node container formats
const (
	quicklistNodeContainerPlain  = 1 // QUICKLIST_NODE_CONTAINER_PLAIN
	quicklistNodeContainerPacked = 2 // QUICKLIST_NODE_CONTAINER_PACKED
)

type ListObject struct {
	Key string

	Elements []string
}

func (o *ListObject) LoadFromBuffer(rd io.Reader, Key string, typeByte byte) {
	o.Key = Key

	switch typeByte {
	case rdbTypeList:
		o.readList(rd)
	case rdbTypeListZiplist:
		o.Elements = structure.ReadZipList(rd)
	case rdbTypeListQuicklist:
		o.readQuickList(rd)
	case rdbTypeListQuicklist2:
		o.readQuickList2(rd)
	default:
		log.Panicf("unknown list type %d", typeByte)
	}
}

func (o *ListObject) Rewrite() []RedisCmd {
	// var cmds []RedisCmd
	cmds := make([]RedisCmd, 0)
	if len(o.Elements) > 1 {
		var cmd RedisCmd
		cmd = append(cmd, "rpush", o.Key)

		for _, v := range o.Elements {
			cmd = append(cmd, v)
		}
		cmds = append(cmds, cmd)
		return cmds
	} else {
		for _, ele := range o.Elements {
			cmd := RedisCmd{"rpush", o.Key, ele}
			cmds = append(cmds, cmd)
		}
		return cmds
	}

}

type ListMember struct {
	Elements []string
	numbers  []int
}

func (n *ListMember) UnmarshalJSON(text []byte) error {

	err := json.Unmarshal(text, &n.Elements)
	if err != nil {
		return json.Unmarshal(text, &n.numbers)
	}

	return err
}

func (o *ListObject) readList(rd io.Reader) {
	var oline ListMember
	ele, err := io.ReadAll(rd)
	if err != nil {
		log.Panicf("readList error : ", err, " when,", &oline)
	}
	err = json.Unmarshal(ele, &oline)

	if err != nil {
		log.Warnf("list.go readList: ", &oline, "   may err:", err)
	}

	for _, e := range oline.Elements {
		o.Elements = append(o.Elements, string(e))
	}
	for _, e := range oline.numbers {
		o.Elements = append(o.Elements, strconv.Itoa(e))
	}

}

func (o *ListObject) readQuickList(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ziplistElements := structure.ReadZipList(rd)
		o.Elements = append(o.Elements, ziplistElements...)
	}
}

func (o *ListObject) readQuickList2(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		container := structure.ReadLength(rd)
		if container == quicklistNodeContainerPlain {
			ele := structure.ReadString(rd)
			o.Elements = append(o.Elements, ele)
		} else if container == quicklistNodeContainerPacked {
			listpackElements := structure.ReadListpack(rd)
			o.Elements = append(o.Elements, listpackElements...)
		} else {
			log.Panicf("unknown quicklist container %d", container)
		}
	}
}

func (o *ListObject) AddList(listlines []string) {
	o.Elements = listlines
}
