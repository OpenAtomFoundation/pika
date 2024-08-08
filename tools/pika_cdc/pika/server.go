package pika

import (
	"bufio"
	"github.com/sirupsen/logrus"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	stop             chan bool
	pikaConn         net.Conn
	pikaAddr         string
	MsgChan          *chan []byte
	pikaReplProtocol ReplProtocol
	writer           *bufio.Writer
	reader           *bufio.Reader
}

// Use todo(leehao): add middleware here
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

func New(s string, msgChan *chan []byte) (Server, error) {
	server := Server{}
	if msgChan == nil {
		ch := make(chan []byte, 10)
		server.MsgChan = &ch
	}
	conn, err := net.Dial("tcp", s)
	if err != nil {
		log.Fatal("Error connecting to Pika server:", err)
	}
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
	return server, err
}

// Run This method will block execution until an error occurs
func (s *Server) Run() {
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(100 * time.Millisecond):
			bytes, _ := s.pikaReplProtocol.GetBinlogSync()
			if len(bytes) != 0 {
				logrus.Info("get a pika binlog send to msg chan")
				*s.MsgChan <- bytes
			}
		}
	}
}

func (s *Server) Exit() {
	s.stop <- true
	close(s.stop)
	close(*s.MsgChan)
}

func (s *Server) CreateSyncWithPika() error {
	//ping := s.pikaReplProtocol.Ping()
	//logrus.Info(ping)
	return s.pikaReplProtocol.GetSyncWithPika()
}
