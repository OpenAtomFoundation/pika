package consumer

import (
	"pika_cdc/conf"
)

type Consumer interface {
	SendCmdMessage(msg []byte) error
	Name() string
	Close() error
	Run()
	Stop()
}

type Factory struct{}

func GenerateConsumers(config conf.PikaCdcConfig, msgChan *chan []byte) ([]Consumer, error) {
	var consumers []Consumer
	kafka, _ := NewKafka(config.KafkaServers, config.Topic, config.Retries)
	consumers = append(consumers, kafka)
	for _, r := range config.RedisServers {
		newRedis, _ := NewRedis(r, msgChan)
		consumers = append(consumers, newRedis)
	}
	return consumers, nil
}
