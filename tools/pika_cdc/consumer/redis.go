package consumer

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

type Redis struct {
	redisProtocol RedisProtocol
	conns         map[string]net.Conn
	msgChanns     map[string]chan []byte
	stopChan      chan bool
}

func NewRedis(addr string, msgChanns map[string]chan []byte) (*Redis, error) {
	r := &Redis{redisProtocol: RedisProtocol{}, conns: make(map[string]net.Conn), msgChanns: msgChanns, stopChan: make(chan bool)}
	var err error
	for dbName, _ := range msgChanns {
		r.conns[dbName], err = net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Redis server: %v", err)
		}
		selectCmdBytes := r.redisProtocol.Select(dbName)
		r.conns[dbName].Write(selectCmdBytes)
	}
	return r, nil
}

func (r *Redis) SendCmdMessage(dbName string, msg []byte) error {
	_, err := r.sendRedisData(dbName, msg)
	return err
}

func (r *Redis) Name() string {
	return string("Redis")
}
func (r *Redis) Close() error {
	for _, conn := range r.conns {
		conn.Close()
	}
	return nil
}

func (r *Redis) sendRedisData(dbName string, data []byte) (string, error) {
	r.conns[dbName].SetDeadline(time.Now().Add(5 * time.Second))
	_, err := r.conns[dbName].Write(data)
	if err != nil {
		return "", fmt.Errorf("failed to send data to Redis server: %v", err)
	}
	reader := bufio.NewReader(r.conns[dbName])
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response from Redis server: %v", err)
	}
	return response, nil
}
func (r *Redis) Run() {
	var wg sync.WaitGroup
	for dbName, chann := range r.msgChanns {
		wg.Add(1)
		go func(dbName string, ch chan []byte) {
			defer wg.Done()
			for {
				select {
				case msg := <-ch:
					r.sendRedisData(dbName, msg)
				case <-r.stopChan:
					return
				}
			}
		}(dbName, chann)
	}
	wg.Wait()
}
func (r *Redis) Stop() {
	r.stopChan <- true
}
