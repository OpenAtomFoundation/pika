package types

import (
	"io"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

type StringObject struct {
	value string
	key   string
}

func (o *StringObject) LoadFromBuffer(rd io.Reader, key string, _ byte) {
	o.key = key
	o.value = structure.ReadString(rd)
}

func (o *StringObject) Rewrite() []RedisCmd {
	cmd := RedisCmd{}
	cmd = append(cmd, "set", o.key, o.value)
	return []RedisCmd{cmd}
}
