package pika

import (
	"bufio"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	stop             chan bool
	pikaConn         net.Conn
	pikaAddr         string
	bufferMsgNumber  int
	MsgChanns        map[string]chan []byte
	pikaReplProtocol ReplProtocol
	writer           *bufio.Writer
	reader           *bufio.Reader
}

// Use todo(leehao): middleware can be added here in the future
func Use() {

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

func New(s string, bufferMsgNumber int) (Server, error) {
	server := Server{}
	server.MsgChanns = make(map[string]chan []byte)
	conn, err := net.Dial("tcp", s)
	if err != nil {
		logrus.Fatal("Error connecting to Pika server:", err)
	}
	server.bufferMsgNumber = bufferMsgNumber
	server.pikaConn = conn
	server.writer = bufio.NewWriter(server.pikaConn)
	server.reader = bufio.NewReader(server.pikaConn)
	server.pikaReplProtocol = ReplProtocol{
		writer: server.writer,
		reader: server.reader,
		ip:     getIP(conn.LocalAddr().String()),
		port:   getPort(conn.LocalAddr().String()),
	}
	err = server.CreateSyncWithPika()
	server.buildMsgChann()
	return server, err
}
func (s *Server) buildMsgChann() {
	dbMetaInfo := s.pikaReplProtocol.dbMetaInfo
	for _, dbInfo := range dbMetaInfo.DbsInfo {
		s.MsgChanns[*dbInfo.DbName] = make(chan []byte, s.bufferMsgNumber)
	}
}

// Run This method will block execution until an error occurs
func (s *Server) Run() {
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(100 * time.Millisecond):
			binlogBytes, _ := s.pikaReplProtocol.GetBinlogSync()
			if len(binlogBytes) != 0 {
				for dbName, binlog := range binlogBytes {
					chann, exists := s.MsgChanns[dbName]
					if !exists {
						chann = make(chan []byte, s.bufferMsgNumber)
						s.MsgChanns[dbName] = chann
					}
					chann <- binlog
				}
			}
		}
	}
}

func (s *Server) Exit() {
	s.stop <- true
	close(s.stop)
	for _, chann := range s.MsgChanns {
		close(chann)
	}
}

func (s *Server) CreateSyncWithPika() error {
	return s.pikaReplProtocol.GetSyncWithPika()
}
