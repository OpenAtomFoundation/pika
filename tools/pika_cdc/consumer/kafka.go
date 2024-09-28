package consumer

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync"
)

type Kafka struct {
	servers    string
	topics     []string
	retries    int
	kafkaConns map[string]*kafka.Conn
	wg         sync.WaitGroup
	msgChanns  map[string]chan []byte
	stopChan   chan bool
	protocol   KafkaProtocol
}

func (k *Kafka) SendCmdMessage(dbName string, msg []byte) error {
	k.kafkaConns[dbName].Write(k.protocol.ToConsumer(msg))
	return nil
}

func (k *Kafka) Name() string {
	return "Kafka"
}

func NewKafka(server string, retries int, msgChanns map[string]chan []byte) (*Kafka, error) {
	k := &Kafka{}
	k.protocol = KafkaProtocol{}
	k.kafkaConns = make(map[string]*kafka.Conn)
	k.msgChanns = make(map[string]chan []byte)
	for dbname, chann := range msgChanns {
		conn, err := kafka.DialLeader(context.Background(), "tcp", server, dbname, 0)
		if err != nil {
			return k, err
		} else {
			k.kafkaConns[dbname] = conn
		}
		k.msgChanns[dbname] = chann
	}
	k.stopChan = make(chan bool)
	k.retries = retries
	return k, nil
}

func (k *Kafka) Close() error {
	k.Stop()
	for _, conn := range k.kafkaConns {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (k *Kafka) Run() {
	var wg sync.WaitGroup
	for dbName, chann := range k.msgChanns {
		wg.Add(1)
		go func(dbName string, ch chan []byte) {
			defer wg.Done()
			for {
				select {
				case msg := <-ch:
					k.SendCmdMessage(dbName, msg)
				case <-k.stopChan:
					return
				}
			}
		}(dbName, chann)
	}
	wg.Wait()
}
func (k *Kafka) Stop() {
	k.stopChan <- true
}
