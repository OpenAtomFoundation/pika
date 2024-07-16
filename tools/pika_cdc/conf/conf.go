package conf

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"strings"
)

type PikaCdcConfig struct {
	Servers []string `yaml:"servers"`
	Topic   string   `yaml:"topic"`
	Retries int      `yaml:"retries"`
}

var ConfigInstance = PikaCdcConfig{}

func init() {
	file, err := ioutil.ReadFile("conf/cdc.yml")
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
		Servers string `yaml:"servers"`
		Topic   string `yaml:"topic"`
		Retries int    `yaml:"retries"`
	}

	if err := unmarshal(&tmp); err != nil {
		return err
	}

	c.Servers = strings.Split(tmp.Servers, ",")
	c.Retries = tmp.Retries
	c.Topic = tmp.Topic

	return nil
}
