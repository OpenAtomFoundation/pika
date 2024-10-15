package main

import (
	"github.com/sirupsen/logrus"
	"pika_cdc/conf"
	"pika_cdc/consumer"
	"pika_cdc/pika"
)

func main() {
	if pikaServer, err := pika.New(conf.ConfigInstance.PikaServer, conf.ConfigInstance.BufferMsgNumbers); err != nil {
		logrus.Fatal("failed to connect pika server, {}", err)
	} else {
		if consumers, err := consumer.GenerateConsumers(conf.ConfigInstance, pikaServer.MsgChanns); err != nil {
			logrus.Fatal("failed to generate consumers, {}", err)
		} else {
			for _, c := range consumers {
				go c.Run()
			}
		}
		pikaServer.Run()
	}
}
