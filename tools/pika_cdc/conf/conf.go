package conf

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

type PikaCdcConfig struct {
	PikaServer         string   `yaml:"pika_server"`
	MqServers          []string `yaml:"mq_servers"`
	Topic              string   `yaml:"topic"`
	Retries            int      `yaml:"retries"`
	RetryInterval      int      `yaml:"retry_interval"`
	ParallelThreadSize int      `yaml:"parallel_thread_size"`
}

var ConfigInstance = PikaCdcConfig{}

func init() {
	_, filename, _, _ := runtime.Caller(0)
	filename = filepath.Join(filepath.Dir(filename), "cdc.yml")
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("fail to read file:", err)
	}

	err = yaml.Unmarshal(file, &ConfigInstance)
	if err != nil {
		log.Fatal("fail to yaml unmarshal:", err)
	}
}

func (c *PikaCdcConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var tmp struct {
		PikaServer         string `yaml:"pika_server"`
		MqServers          string `yaml:"mq_servers"`
		Topic              string `yaml:"topic"`
		Retries            int    `yaml:"retries"`
		RetryInterval      int    `yaml:"retry_interval"`
		ParallelThreadSize int    `yaml:"parallel_thread_size"`
	}

	if err := unmarshal(&tmp); err != nil {
		return err
	}

	c.MqServers = strings.Split(tmp.MqServers, ",")
	c.Retries = tmp.Retries
	c.Topic = tmp.Topic
	c.PikaServer = tmp.PikaServer

	return nil
}
