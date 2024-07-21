package pika

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"net"
	"os"
	"pika_cdc/conf"
	"pika_cdc/pika/proto/inner"
	"strconv"
	"strings"
	"testing"
)

func TestConnect(t *testing.T) {
	cxt := context.Background()
	addr := "127.0.0.1:9221"
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	fmt.Println(client.Get(cxt, "key"))
}

func getPort(addr string) int32 {
	portStr := addr[strings.LastIndex(addr, ":")+1:]
	port, _ := strconv.Atoi(portStr)
	return int32(port)
}

func TestSendMetaSync(t *testing.T) {
	ip := string("127.0.0.1")
	listener, e := net.Listen("tcp", ":0")
	if e != nil {
		os.Exit(1)
	}
	selfPort := getPort(listener.Addr().String())
	var masterPort int32 = getPort(conf.ConfigInstance.PikaServer) + 2000
	addr := ip + ":" + strconv.Itoa(int(masterPort))
	tt := inner.Type_kMetaSync
	request := inner.InnerRequest{
		Type: &tt,
		MetaSync: &inner.InnerRequest_MetaSync{
			Node: &inner.Node{
				Ip:   &ip,
				Port: &selfPort,
			},
		},
	}
	msg, err := proto.Marshal(&request)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Error connecting:", err)
		os.Exit(1)
	}
	defer conn.Close()

	pikaTag := []byte(BuildInternalTag(msg))
	allBytes := append(pikaTag, msg...)
	_, err = conn.Write(allBytes)
	if err != nil {
		fmt.Println("Error writing to server:", err)
		os.Exit(1)
	}
}

func BuildInternalTag(resp []byte) (tag string) {
	respSize := uint32(len(resp))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, respSize)
	return string(buf)
}
