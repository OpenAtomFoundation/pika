package consumer

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type Redis struct {
	protocol Protocol
	conn     net.Conn
	msgChan  *chan []byte
}

func NewRedis(addr string, msgChan *chan []byte) (*Redis, error) {
	r := &Redis{protocol: RedisProtocol{}, msgChan: msgChan}
	var err error
	r.conn, err = net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis server: %v", err)
	}
	return r, nil
}

func (r *Redis) SendCmdMessage(msg []byte) error {
	_, err := r.sendRedisData(msg)
	return err
}

func (r *Redis) Name() string {
	return string("Redis")
}
func (r *Redis) Close() error {
	return r.conn.Close()
}

func (r *Redis) sendRedisData(data []byte) (string, error) {

	r.conn.SetDeadline(time.Now().Add(5 * time.Second))

	_, err := r.conn.Write(data)
	if err != nil {
		return "", fmt.Errorf("failed to send data to Redis server: %v", err)
	}

	reader := bufio.NewReader(r.conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response from Redis server: %v", err)
	}

	return response, nil
}
func (r *Redis) Run() {
	for msg := range *r.msgChan {
		r.sendRedisData(msg)
	}
}
func (r *Redis) Stop() {

}
