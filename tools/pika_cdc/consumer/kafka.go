package consumer

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"log"
	"sync"
	"time"
)

type Kafka struct {
	servers     []string
	topic       string
	retries     int
	conns       []*kafka.Conn
	wg          sync.WaitGroup
	messageChan chan kafka.Message
	stopChan    chan bool
	once        sync.Once
	protocol    Protocol
}

func (k *Kafka) SendCmdMessage(msg []byte) error {
	select {
	case k.messageChan <- kafka.Message{Value: k.protocol.ToConsumer(msg)}:
		return nil
	case <-time.After(2 * time.Second):
		e := errors.New("send pika cmd timeout")
		logrus.Warn("{}", e)
		return e
	}
}

func (k *Kafka) sendMessage() {
	for {
		select {
		case msg := <-k.messageChan:
			for _, conn := range k.conns {
				_, _ = conn.WriteMessages(msg)
			}
		case _ = <-k.stopChan:
			return
		}
	}
}

func (k *Kafka) Name() string {
	return "Kafka"
}

func NewKafka(servers []string, topic string, retries int) (*Kafka, error) {
	k := &Kafka{}
	k.protocol = &KafkaProtocol{}
	for _, server := range servers {
		conn, err := kafka.DialLeader(context.Background(), "tcp", server, topic, 0)
		if err != nil {
			return k, err
		} else {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			k.conns = append(k.conns, conn)
		}
	}
	k.messageChan = make(chan kafka.Message)
	k.stopChan = make(chan bool)
	go k.sendMessage()
	return k, nil
}

func (k *Kafka) close() error {
	k.stopChan <- true
	close(k.stopChan)
	close(k.messageChan)
	for _, conn := range k.conns {
		err := conn.Close()
		if err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}
func (k *Kafka) Close() error {
	var err error
	err = nil
	k.once.Do(func() {
		err = k.close()
	})
	return err
}
func (k *Kafka) Run() {

}
func (k *Kafka) Stop() {

}
