package mq

import (
	"pika_cdc/conf"
	"pika_cdc/pika"
)

type Mq interface {
	SendCmdMessage(cmd pika.Cmd) error
	Name() string
	Close() error
}

type Factory struct{}
type Type int

const (
	KAFKA = iota
	PULSAR
)

func (f *Factory) GetMq(t Type, config conf.PikaCdcConfig) (Mq, error) {
	switch t {
	case KAFKA:
		return NewKafka(config.Servers, config.Topic, config.Retries)
	default:

	}
	panic("unimplemented")
}
