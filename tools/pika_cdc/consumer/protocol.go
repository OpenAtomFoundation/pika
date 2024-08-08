package consumer

import (
	"fmt"
	"strconv"
)

type Protocol interface {
	ToConsumer(msg []byte) []byte
}
type RedisProtocol struct{}

func (rp RedisProtocol) ToConsumer(msg []byte) []byte {
	return msg
}
func (rp RedisProtocol) Select(dbName string) []byte {
	db, _ := strconv.Atoi(dbName[len(dbName)-1:])
	dbStr := strconv.Itoa(db)
	msg := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)
	return []byte(msg)
}

type KafkaProtocol struct{}

func (kp KafkaProtocol) ToConsumer(msg []byte) []byte {
	return msg
}
