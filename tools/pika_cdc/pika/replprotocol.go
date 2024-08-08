package pika

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"io"
	"pika_cdc/pika/proto/inner"
)

const HeaderLength = 4

type ReplProtocol struct {
	writer          *bufio.Writer
	reader          *bufio.Reader
	binlogSyncInfos []binlogSyncInfo
	dbMetaInfo      *inner.InnerResponse_MetaSync
	ip              string
	port            int32
}

type binlogSyncInfo struct {
	binlogOffset *inner.BinlogOffset
	fileNum      uint32
	offset       uint64
	sessionId    int32
	isFirst      bool
}

func (repl *ReplProtocol) GetSyncWithPika() error {
	if err := repl.sendMetaSyncRequest(); err != nil {
		return err
	}
	metaResp := repl.getResponse()
	if metaResp == nil {
		logrus.Fatal("Failed to get metaResp")
	}
	repl.dbMetaInfo = metaResp.MetaSync

	trySyncType := inner.Type_kTrySync
	binlogSyncType := inner.Type_kBinlogSync

	replDBs := metaResp.MetaSync.DbsInfo
	var a uint64 = 0
	var b uint32 = 0
	for _, dbInfo := range replDBs {
		newMetaInfo := binlogSyncInfo{
			binlogOffset: &inner.BinlogOffset{
				Filenum: nil,
				Offset:  nil,
				Term:    nil,
				Index:   nil,
			},
			fileNum:   0,
			offset:    0,
			sessionId: 0,
		}
		newMetaInfo.binlogOffset.Offset = &a
		newMetaInfo.binlogOffset.Filenum = &b

		slotId := uint32(*dbInfo.SlotNum)
		trySync := &inner.InnerRequest{
			Type: &trySyncType,
			TrySync: &inner.InnerRequest_TrySync{
				Node: &inner.Node{
					Ip:   &repl.ip,
					Port: &repl.port,
				},
				Slot: &inner.Slot{
					DbName: dbInfo.DbName,
					SlotId: &slotId,
				},
				BinlogOffset: newMetaInfo.binlogOffset,
			},
			ConsensusMeta: nil,
		}
		if err := repl.sendReplReq(trySync); err != nil {
			return err
		}

		trySyncResp := repl.getResponse()
		if trySyncResp == nil || *trySyncResp.Code != inner.StatusCode_kOk {
			logrus.Fatal("Failed to get TrySync Response Msg")
		}
		startOffset := trySyncResp.TrySync.GetBinlogOffset()
		trySync.TrySync.BinlogOffset = startOffset
		// send twice to get session id
		if err := repl.sendReplReq(trySync); err != nil {
			return err
		}
		trySyncResp = repl.getResponse()

		newMetaInfo.binlogOffset = startOffset
		newMetaInfo.sessionId = *trySyncResp.TrySync.SessionId
		newMetaInfo.isFirst = true
		repl.binlogSyncInfos = append(repl.binlogSyncInfos, newMetaInfo)
	}

	// todo(leehao): Can find ways to optimize using coroutines here. May be use goroutine
	for index, dbInfo := range repl.binlogSyncInfos {
		slotId := uint32(*repl.dbMetaInfo.DbsInfo[index].SlotNum)
		binlogSyncReq := &inner.InnerRequest{
			Type:     &binlogSyncType,
			MetaSync: nil,
			TrySync:  nil,
			DbSync:   nil,
			BinlogSync: &inner.InnerRequest_BinlogSync{
				Node: &inner.Node{
					Ip:   &repl.ip,
					Port: &repl.port,
				},
				DbName:        repl.dbMetaInfo.DbsInfo[index].DbName,
				SlotId:        &slotId,
				AckRangeStart: dbInfo.binlogOffset,
				AckRangeEnd:   dbInfo.binlogOffset,
				SessionId:     &dbInfo.sessionId,
				FirstSend:     &dbInfo.isFirst,
			},
			RemoveSlaveNode: nil,
			ConsensusMeta:   nil,
		}
		if err := repl.sendReplReq(binlogSyncReq); err != nil {
			return err
		}
		repl.binlogSyncInfos[index].isFirst = false
	}
	return nil
}

func (repl *ReplProtocol) GetBinlogSync() ([]byte, error) {

	binlogSyncType := inner.Type_kBinlogSync
	var binlogByte []byte
	// todo(leehao): Receive multiple binlog sync responses simultaneously
	binlogSyncResp := repl.getResponse()
	if binlogSyncResp == nil || *binlogSyncResp.Code != inner.StatusCode_kOk ||
		*binlogSyncResp.Type != inner.Type_kBinlogSync || binlogSyncResp.BinlogSync == nil {
		logrus.Fatal("get binlog sync response failed")
	} else {
		for index, item := range binlogSyncResp.BinlogSync {
			slotId := uint32(*repl.dbMetaInfo.DbsInfo[index].SlotNum)
			binlogOffset := repl.binlogSyncInfos[index].binlogOffset
			if len(item.Binlog) == 0 {
				*binlogOffset.Filenum = 0
				*binlogOffset.Offset = 0
				logrus.Println("receive binlog response keep alive")
			} else {
				binlogOffset = item.BinlogOffset
				repl.binlogSyncInfos[index].binlogOffset = binlogOffset
				if binlogItem, err := repl.decodeBinlogItem(item.Binlog); err != nil {
					logrus.Fatal(err)
				} else {
					binlogByte = binlogItem.Content
				}
			}
			err := repl.sendReplReq(&inner.InnerRequest{
				Type:     &binlogSyncType,
				MetaSync: nil,
				TrySync:  nil,
				DbSync:   nil,
				BinlogSync: &inner.InnerRequest_BinlogSync{
					Node: &inner.Node{
						Ip:   &repl.ip,
						Port: &repl.port,
					},
					DbName:        repl.dbMetaInfo.DbsInfo[index].DbName,
					SlotId:        &slotId,
					AckRangeStart: binlogOffset,
					AckRangeEnd:   binlogOffset,
					SessionId:     &repl.binlogSyncInfos[index].sessionId,
					FirstSend:     &repl.binlogSyncInfos[index].isFirst,
				},
				RemoveSlaveNode: nil,
				ConsensusMeta:   nil,
			})
			if err != nil {
				logrus.Warn("Failed to send binlog sync, {}", err)
				return nil, err
			}
		}
	}
	return binlogByte, nil
}

func (repl *ReplProtocol) Ping() string {
	_, err := repl.writer.WriteString("PING\r\n")
	if err != nil {
		logrus.Warn("Error writing to connection:", err)
		return string("")
	}
	repl.writer.Flush()

	resp, err := repl.reader.ReadString('\n')
	if err != nil {
		logrus.Warn("Error reading from connection:", err)
		return string("")
	}
	return resp
}

func (repl *ReplProtocol) sendMetaSyncRequest() error {
	logrus.Info("sendMetaSyncRequest")
	metaSyncType := inner.Type_kMetaSync
	request := &inner.InnerRequest{
		Type: &metaSyncType,
		MetaSync: &inner.InnerRequest_MetaSync{
			Node: &inner.Node{
				Ip:   &repl.ip,
				Port: &repl.port,
			},
		},
	}
	return repl.sendReplReq(request)
}

func (repl *ReplProtocol) sendReplReq(request *inner.InnerRequest) error {
	msg, err := proto.Marshal(request)
	if err != nil {
		logrus.Fatal("Error Marshal:", err)
	}

	pikaTag := []byte(repl.buildInternalTag(msg))
	allBytes := append(pikaTag, msg...)
	_, err = repl.writer.Write(allBytes)
	if err != nil {
		logrus.Fatal("Error writing to server:", err)
	}
	repl.writer.Flush()
	return nil
}

func (repl *ReplProtocol) getResponse() *inner.InnerResponse {
	header := make([]byte, HeaderLength)
	_, err := repl.reader.Read(header)
	if err != nil {
		if err != io.EOF {
			logrus.Fatal("Error reading header:", err)
		}
		return nil
	}

	// Convert the header to an integer
	var bodyLength uint32
	buffer := bytes.NewBuffer(header)
	err = binary.Read(buffer, binary.BigEndian, &bodyLength)
	if err != nil {
		logrus.Fatal("Error converting header to integer:", err)
		return nil
	}
	// Read the body
	body := make([]byte, bodyLength)
	_, err = repl.reader.Read(body)
	if err != nil {
		logrus.Fatal("Error reading body:", err)
		return nil
	}

	res := &inner.InnerResponse{}
	err = proto.Unmarshal(body, res)
	if err != nil {
		logrus.Fatal("Error Deserialization:", err)
	}
	return res
}

func (repl *ReplProtocol) buildInternalTag(resp []byte) (tag string) {
	respSize := uint32(len(resp))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, respSize)
	return string(buf)
}

type binlogItem struct {
	Type          uint16
	CreateTime    uint32
	TermId        uint32
	LogicId       uint64
	FileNum       uint32
	Offset        uint64
	ContentLength uint32
	Content       []byte
}

func (repl *ReplProtocol) decodeBinlogItem(data []byte) (*binlogItem, error) {
	if len(data) < 34 {
		return nil, fmt.Errorf("data length is too short")
	}

	reader := bytes.NewReader(data)

	binlogItem := &binlogItem{}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.Type); err != nil {
		return nil, fmt.Errorf("failed to read Type: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.CreateTime); err != nil {
		return nil, fmt.Errorf("failed to read Create Time: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.TermId); err != nil {
		return nil, fmt.Errorf("failed to read Term Id: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.LogicId); err != nil {
		return nil, fmt.Errorf("failed to read Logic Id: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.FileNum); err != nil {
		return nil, fmt.Errorf("failed to read File Num: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.Offset); err != nil {
		return nil, fmt.Errorf("failed to read Offset: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &binlogItem.ContentLength); err != nil {
		return nil, fmt.Errorf("failed to read Content Length: %v", err)
	}

	contentLength := int(binlogItem.ContentLength)
	if len(data) < 34+contentLength {
		return nil, fmt.Errorf("data length is too short for content")
	}

	binlogItem.Content = make([]byte, contentLength)
	if _, err := reader.Read(binlogItem.Content); err != nil {
		return nil, fmt.Errorf("failed to read Content: %v", err)
	}

	return binlogItem, nil
}
