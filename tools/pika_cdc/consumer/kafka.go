package consumer

import (
	"github.com/segmentio/kafka-go"
	"sync"
)

type Kafka struct {
	servers   []string
	topic     string
	retries   int
	conns     []*kafka.Conn
	wg        sync.WaitGroup
	msgChanns map[string]chan []byte
	stopChan  chan bool
	once      sync.Once
	protocol  Protocol
}

func (k *Kafka) SendCmdMessage(dbName string, msg []byte) error {
	//retries := k.retries
	//select {
	//case *k.messageChan <- k.protocol.ToConsumer(msg):
	//	return nil
	//case <-time.After(2 * time.Second):
	//	e := errors.New("send pika cmd timeout and retry send pika cmd")
	//	logrus.Warn("{}", e)
	//	retries--
	//	if retries <= 0 {
	//		break
	//	}
	//}
	return nil
}

func (k *Kafka) Name() string {
	return "Kafka"
}

func NewKafka(servers []string, topic string, retries int, msgChanns map[string]chan []byte) (*Kafka, error) {
	k := &Kafka{}
	//k.protocol = &KafkaProtocol{}
	//for _, server := range servers {
	//	conn, err := kafka.DialLeader(context.Background(), "tcp", server, topic, 0)
	//	if err != nil {
	//		return k, err
	//	} else {
	//		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	//		k.conns = append(k.conns, conn)
	//	}
	//}
	//k.messageChan = msgChanns
	//k.stopChan = make(chan bool)
	//k.retries = retries
	return k, nil
}

func (k *Kafka) close() error {
	//k.stopChan <- true
	//close(k.stopChan)
	//close(*k.messageChan)
	//for _, conn := range k.conns {
	//	err := conn.Close()
	//	if err != nil {
	//		logrus.Warn(err)
	//		return err
	//	}
	//}
	return nil
}
func (k *Kafka) Close() error {
	//var err error
	//err = nil
	//k.once.Do(func() {
	//	err = k.close()
	//})
	//return err
	return nil
}
func (k *Kafka) Run() {
	//select {
	//case msg := <-*k.messageChan:
	//	k.SendCmdMessage(msg)
	//case <-k.stopChan:
	//	return
	//}
}
func (k *Kafka) Stop() {
	k.stopChan <- true
}
