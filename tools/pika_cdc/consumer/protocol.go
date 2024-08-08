package consumer

type Protocol interface {
	ToConsumer(msg []byte) []byte
}
type RedisProtocol struct{}

func (rp RedisProtocol) ToConsumer(msg []byte) []byte {
	return msg
}

type KafkaProtocol struct{}

func (kp KafkaProtocol) ToConsumer(msg []byte) []byte {
	return nil
}
