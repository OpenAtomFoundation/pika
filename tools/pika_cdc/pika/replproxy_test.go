package pika

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
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
func getIP(addr string) string {
	index := strings.LastIndex(addr, ":")
	if index == -1 {
		return addr
	}
	return addr[:index]
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

const HeaderLength = 4

func receiveReplMsg(listener net.Listener) {
	defer listener.Close()
	fmt.Println("Listening on ", listener.Addr().String())
	for {
		conn, err := listener.Accept()
		fmt.Println(conn.LocalAddr().String() + " connect")
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		//go handleConnection(conn)
	}
}
func getResponse(conn net.Conn) *inner.InnerResponse {
	// Read the header (length)
	header := make([]byte, HeaderLength)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		if err != io.EOF {
			fmt.Println("Error reading header:", err)
		}
		return nil
	}

	// Convert the header to an integer
	var bodyLength uint32
	buffer := bytes.NewBuffer(header)
	err = binary.Read(buffer, binary.BigEndian, &bodyLength)
	if err != nil {
		log.Fatal("Error converting header to integer:", err)
		return nil
	}
	// Read the body
	body := make([]byte, bodyLength)
	_, err = io.ReadFull(conn, body)
	if err != nil {
		log.Fatal("Error reading body:", err)
		return nil
	}

	res := &inner.InnerResponse{}
	err = proto.Unmarshal(body, res)
	if err != nil {
		log.Fatal("Error Deserialization:", err)
	}
	return res
}

func sendReplReq(conn net.Conn, request *inner.InnerRequest) (net.Conn, error) {
	if conn == nil {
		ip := string("127.0.0.1")
		var masterReplPort int32 = getPort(conf.ConfigInstance.PikaServer) + 2000
		addr := ip + ":" + strconv.Itoa(int(masterReplPort))
		newConn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		conn = newConn
	}
	msg, err := proto.Marshal(request)
	if err != nil {
		log.Fatal("Error Marshal:", err)
	}

	pikaTag := []byte(BuildInternalTag(msg))
	allBytes := append(pikaTag, msg...)
	_, err = conn.Write(allBytes)
	if err != nil {
		log.Fatal("Error writing to server:", err)
	}
	return conn, nil
}

func sendMetaSyncRequest(conn net.Conn) (net.Conn, error) {
	if conn == nil {
		ip := string("127.0.0.1")
		var masterReplPort int32 = getPort(conf.ConfigInstance.PikaServer) + 2000
		addr := ip + ":" + strconv.Itoa(int(masterReplPort))
		newConn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		conn = newConn
	}
	metaSyncType := inner.Type_kMetaSync
	port := getPort(conn.LocalAddr().String())
	ip := getIP(conn.LocalAddr().String())
	request := &inner.InnerRequest{
		Type: &metaSyncType,
		MetaSync: &inner.InnerRequest_MetaSync{
			Node: &inner.Node{
				Ip:   &ip,
				Port: &port,
			},
		},
	}
	return sendReplReq(conn, request)
}

func TestGetOffsetFromMaster(t *testing.T) {
	ip := string("127.0.0.1")
	listener, e := net.Listen("tcp", ":0")
	if e != nil {
		os.Exit(1)
	}
	selfPort := getPort(listener.Addr().String())
	conn, err := sendMetaSyncRequest(nil)
	if err != nil {
		log.Fatal("Failed to sendMetaSyncRequest")
	}
	metaResp := getResponse(conn)
	trySyncType := inner.Type_kTrySync
	replDBs := metaResp.MetaSync.DbsInfo
	var fileNum uint32 = 1
	var offset uint64 = 0
	for _, db := range replDBs {
		slotId := uint32(*db.SlotNum)
		trySync := &inner.InnerRequest{
			Type: &trySyncType,
			TrySync: &inner.InnerRequest_TrySync{
				Node: &inner.Node{
					Ip:   &ip,
					Port: &selfPort,
				},
				Slot: &inner.Slot{
					DbName: db.DbName,
					SlotId: &slotId,
				},
				BinlogOffset: &inner.BinlogOffset{
					Filenum: &fileNum,
					Offset:  &offset,
					Term:    nil,
					Index:   nil,
				},
			},
			ConsensusMeta: nil,
		}
		_, err = sendReplReq(conn, trySync)
		if err != nil {
			log.Fatal("Failed to send TrySync Msg", err)
		}
		trySyncResp := getResponse(conn)
		if trySyncResp == nil || *trySyncResp.Code != inner.StatusCode_kOk {
			log.Fatal("Failed to get TrySync Response Msg", err)
		}
		trySync.TrySync.BinlogOffset = trySyncResp.TrySync.GetBinlogOffset()
		log.Println("get offset:", trySync.TrySync.BinlogOffset)
	}
}

func TestSendDbSyncReqMsg(t *testing.T) {
	ip := string("127.0.0.1")
	listener, e := net.Listen("tcp", ":0")
	if e != nil {
		os.Exit(1)
	}

	selfPort := getPort(listener.Addr().String())

	metaSyncType := inner.Type_kMetaSync

	request := &inner.InnerRequest{
		Type: &metaSyncType,
		MetaSync: &inner.InnerRequest_MetaSync{
			Node: &inner.Node{
				Ip:   &ip,
				Port: &selfPort,
			},
		},
	}
	conn, err := sendReplReq(nil, request)
	if err != nil {
		os.Exit(1)
	}
	metaResp := getResponse(conn)

	dbSyncType := inner.Type_kDBSync
	replDBs := metaResp.MetaSync.DbsInfo
	for _, db := range replDBs {
		var fileNum uint32 = 1
		var offset uint64 = 0
		slotId := uint32(*db.SlotNum)
		dbSyncReq := &inner.InnerRequest{
			Type: &dbSyncType,
			DbSync: &inner.InnerRequest_DBSync{
				Node: &inner.Node{
					Ip:   &ip,
					Port: &selfPort,
				},
				Slot: &inner.Slot{
					DbName: db.DbName,
					SlotId: &slotId,
				},
				BinlogOffset: &inner.BinlogOffset{
					Filenum: &fileNum,
					Offset:  &offset,
					Term:    nil,
					Index:   nil,
				},
			},
			ConsensusMeta: nil,
		}
		sendReplReq(conn, dbSyncReq)
	}
}

func BuildInternalTag(resp []byte) (tag string) {
	respSize := uint32(len(resp))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, respSize)
	return string(buf)
}

func TestGetIncrementalSync(t *testing.T) {
	conn, err := sendMetaSyncRequest(nil)
	if err != nil {
		log.Fatal(err)
	}
	metaResp := getResponse(conn)
	if metaResp == nil {
		log.Fatal("Failed to get metaResp")
	}
	trySyncType := inner.Type_kTrySync
	replDBs := metaResp.MetaSync.DbsInfo
	var fileNum uint32 = 1
	var offset uint64 = 0
	ip := getIP(conn.LocalAddr().String())
	port := getPort(conn.LocalAddr().String())

	for _, db := range replDBs {
		slotId := uint32(*db.SlotNum)
		trySync := &inner.InnerRequest{
			Type: &trySyncType,
			TrySync: &inner.InnerRequest_TrySync{
				Node: &inner.Node{
					Ip:   &ip,
					Port: &port,
				},
				Slot: &inner.Slot{
					DbName: db.DbName,
					SlotId: &slotId,
				},
				BinlogOffset: &inner.BinlogOffset{
					Filenum: &fileNum,
					Offset:  &offset,
					Term:    nil,
					Index:   nil,
				},
			},
			ConsensusMeta: nil,
		}
		_, err = sendReplReq(conn, trySync)
		if err != nil {
			log.Fatal("Failed to send TrySync Msg", err)
		}
		trySyncResp := getResponse(conn)
		if trySyncResp == nil || *trySyncResp.Code != inner.StatusCode_kOk {
			log.Fatal("Failed to get TrySync Response Msg", err)
		}
		// todo(leehao) test send BinlogSync to get Incremental data
		binlogSyncReq := &inner.InnerRequest_BinlogSync{
			Node:          nil,
			DbName:        nil,
			SlotId:        nil,
			AckRangeStart: nil,
			AckRangeEnd:   nil,
			SessionId:     nil,
			FirstSend:     nil,
		}
		trySync.TrySync.BinlogOffset = trySyncResp.TrySync.GetBinlogOffset()
	}
}
