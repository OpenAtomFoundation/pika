package pika_keys_analysis

import (
	"os"

	"gopkg.in/yaml.v3"
)

var (
	PikaInstance *Pika
	ScanSize     = 1000
	GoroutineNum = 100
	Head         = 10
	Type         = []string{"string", "hash", "list", "set", "zset"}
	MemoryLimit  = 1024 * 1024 * 200
	PrintKeyNum  = false
)

type Config struct {
	PikaConfig  []PikaConfig `yaml:"pika"`
	Concurrency int          `yaml:"concurrency"`
	ScanSize    int          `yaml:"scan-size"`
	Head        int          `yaml:"head"`
	MemoryLimit int          `yaml:"memory"`
	Type        []string     `yaml:"type"`
	PrintKeyNum bool         `yaml:"print"`
}

type PikaConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

func Init(filename string) error {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	config := Config{}
	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		return err
	}
	PikaInstance = NewPika(config.PikaConfig)
	ScanSize = config.ScanSize
	GoroutineNum = config.Concurrency
	Head = config.Head
	Type = config.Type
	MemoryLimit = config.MemoryLimit * 1024 * 1024
	PrintKeyNum = config.PrintKeyNum
	return nil
}
