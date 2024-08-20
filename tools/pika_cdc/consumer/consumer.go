package consumer

import (
	"pika_cdc/conf"
)

type Consumer interface {
	SendCmdMessage(dbName string, msg []byte) error
	Name() string
	Close() error
	Run()
	Stop()
}

type Factory struct{}

func GenerateConsumers(config conf.PikaCdcConfig, msgChanns map[string]chan []byte) ([]Consumer, error) {
	var consumers []Consumer

	// kafka
	for _, k := range config.KafkaServers {
		kafka, _ := NewKafka(k, config.Retries, msgChanns)
		consumers = append(consumers, kafka)
	}

	// redis
	for _, r := range config.RedisServers {
		newRedis, _ := NewRedis(r, msgChanns)
		consumers = append(consumers, newRedis)
	}
	return consumers, nil
}
