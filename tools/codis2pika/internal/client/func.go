package client

import (
	"bytes"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/client/proto"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

// 接口函数：校验入参否为string并返回string的切片
func ArrayString(replyInterface interface{}, err error) []string {
	if err != nil {
		log.PanicError(err)
	}
	replyArray := replyInterface.([]interface{})
	replyArrayString := make([]string, len(replyArray))
	for inx, item := range replyArray {
		replyArrayString[inx] = item.(string)
	}
	return replyArrayString
}

func EncodeArgv(argv []string, buf *bytes.Buffer) {
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(argv))
	for inx, item := range argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.PanicError(err)
	}
}
