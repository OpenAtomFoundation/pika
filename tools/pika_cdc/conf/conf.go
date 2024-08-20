package conf

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
)

type PikaCdcConfig struct {
	PikaServer         string   `yaml:"pika_server"`
	KafkaServers       []string `yaml:"kafka_servers"`
	RedisServers       []string `yaml:"redis_servers"`
	PulsarServers      []string `yaml:"pulsar_servers"`
	Retries            int      `yaml:"retries"`
	RetryInterval      int      `yaml:"retry_interval"`
	ParallelThreadSize int      `yaml:"parallel_thread_size"`
	BufferMsgNumbers   int      `yaml:"buffer_msg_numbers"`
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

	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return "", fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
		},
	})

	logrus.SetReportCaller(true)
	logrus.SetOutput(os.Stdout)
}
