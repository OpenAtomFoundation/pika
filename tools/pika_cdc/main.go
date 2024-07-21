package main

import (
	"fmt"
	"pika_cdc/conf"
	"pika_cdc/mq"
	"pika_cdc/pika"
	"time"
)

func main() {
	mqFactory := mq.Factory{}
	messageQue, err := mqFactory.GetMq(mq.KAFKA, conf.ConfigInstance)
	if err != nil {
		panic("failed to create mq, " + err.Error())
	} else {
		fmt.Println(messageQue.Name())
		c := pika.Cmd{}
		_ = messageQue.SendCmdMessage(c)
	}
	defer messageQue.Close()
	time.Sleep(time.Second)

	//pikaServer := pika.New(conf.ConfigInstance.PikaServer)
	//pikaServer.Start()
	//defer pikaServer.Close()
}
