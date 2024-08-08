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
	binlogSyncInfos map[string]binlogSyncInfo
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
	metaResp, err := repl.getResponse()
	if err != nil {
		logrus.Fatal("Failed to get metaResp:", err)
	}
	repl.dbMetaInfo = metaResp.MetaSync

	trySyncType := inner.Type_kTrySync
	binlogSyncType := inner.Type_kBinlogSync

	replDBs := metaResp.MetaSync.DbsInfo
	var a uint64 = 0
	var b uint32 = 0
	repl.binlogSyncInfos = make(map[string]binlogSyncInfo)
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

		trySyncResp, err := repl.getResponse()
		if err != nil || trySyncResp == nil || *trySyncResp.Code != inner.StatusCode_kOk {
			logrus.Fatal("Failed to get TrySync Response Msg")
		}
		startOffset := trySyncResp.TrySync.GetBinlogOffset()
		trySync.TrySync.BinlogOffset = startOffset
		// send twice to get session id
		if err := repl.sendReplReq(trySync); err != nil {
			return err
		}
		trySyncResp, err = repl.getResponse()

		newMetaInfo.binlogOffset = startOffset
		newMetaInfo.sessionId = *trySyncResp.TrySync.SessionId
		newMetaInfo.isFirst = true
		repl.binlogSyncInfos[dbInfo.GetDbName()] = newMetaInfo
	}

	// todo(leehao): Can find ways to optimize using coroutines here. May be use goroutine
	for dbName, dbInfo := range repl.binlogSyncInfos {
		var slotId uint32 = 0
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
				DbName:        &dbName,
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
	}
	return nil
}

func (repl *ReplProtocol) GetBinlogSync() (map[string][]byte, error) {

	binlogSyncType := inner.Type_kBinlogSync
	// This is a collection of binlogs for all DB's
	binlogBytes := make(map[string][]byte)
	// todo(leehao): Receive multiple binlog sync responses simultaneously
	binlogSyncResp, err := repl.getResponse()
	if err != nil {
		return nil, err
	}
	if binlogSyncResp == nil || *binlogSyncResp.Code != inner.StatusCode_kOk ||
		*binlogSyncResp.Type != inner.Type_kBinlogSync || binlogSyncResp.BinlogSync == nil {
		logrus.Fatal("get binlog sync response failed")
	} else {
		for _, item := range binlogSyncResp.BinlogSync {
			slotId := *item.Slot.SlotId
			dbName := *item.Slot.DbName
			binlogInfo := repl.binlogSyncInfos[dbName]
			binlogInfo.isFirst = false
			binlogOffset := item.BinlogOffset
			if len(item.Binlog) == 0 || (*binlogOffset.Offset == binlogInfo.offset && *binlogOffset.Filenum == binlogInfo.fileNum) {
				*binlogOffset.Filenum = 0
				*binlogOffset.Offset = 0
			} else {
				binlogInfo.binlogOffset = binlogOffset
				if binlogItem, err := repl.decodeBinlogItem(item.Binlog); err != nil {
					logrus.Fatal(err)
				} else {
					logrus.Info("recv binlog db:", dbName, " ,size:", len(item.Binlog))
					binlogBytes[dbName] = binlogItem.Content
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
					DbName:        &dbName,
					SlotId:        &slotId,
					AckRangeStart: binlogOffset,
					AckRangeEnd:   binlogOffset,
					SessionId:     &binlogInfo.sessionId,
					FirstSend:     &binlogInfo.isFirst,
				},
				RemoveSlaveNode: nil,
				ConsensusMeta:   nil,
			})
			repl.binlogSyncInfos[dbName] = binlogInfo
			if err != nil {
				logrus.Warn("Failed to send binlog sync, ", err)
				return nil, err
			}
		}
	}
	return binlogBytes, nil
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

func (repl *ReplProtocol) getResponse() (*inner.InnerResponse, error) {
	header := make([]byte, HeaderLength)
	_, err := repl.reader.Read(header)
	if err != nil {
		if err != io.EOF {
			logrus.Fatal("Error reading header:", err)
		}
		return nil, err
	}

	// Convert the header to an integer
	var bodyLength uint32
	buffer := bytes.NewBuffer(header)
	err = binary.Read(buffer, binary.BigEndian, &bodyLength)
	if err != nil {
		logrus.Fatal("Error converting header to integer:", err)
		return nil, err
	}
	// Read the body
	body := make([]byte, bodyLength)
	_, err = repl.reader.Read(body)
	if err != nil {
		logrus.Fatal("Error reading body:", err)
		return nil, err
	}

	res := &inner.InnerResponse{}
	err = proto.Unmarshal(body, res)
	if err != nil {
		logrus.Warn("Error Deserialization:", err)
		return nil, err
	}
	return res, nil
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
