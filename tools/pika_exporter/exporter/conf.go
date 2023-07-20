package exporter

import (
	"fmt"

	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
)

var InfoConf *InfoConfig

var InfoConfigPath string

type InfoConfig struct {
	Server       bool `toml:"server"`
	Data         bool `toml:"data"`
	Clients      bool `toml:"clients"`
	Stats        bool `toml:"stats"`
	CPU          bool `toml:"cpu"`
	Replication  bool `toml:"replication"`
	Keyspace     bool `toml:"keyspace"`
	Execcount    bool `toml:"execcount"`
	Commandstats bool `toml:"commandstats"`
	Rocksdb      bool `toml:"rocksdb"`

	Info    bool
	InfoAll bool
}

func LoadConfig() error {
	log.Println("Update configuration")
	err := readConfig(InfoConfigPath)
	if err != nil {
		return err
	}

	InfoConf.CheckInfo()
	InfoConf.Display()

	return nil
}

func readConfig(filePath string) error {
	conf := InfoConfig{}
	tree, err := toml.LoadFile(filePath)
	if err != nil {
		return fmt.Errorf("unable to load toml file %s: %s", filePath, err)
	}

	// Unmarshal the TOML data into the Config struct
	err = tree.Unmarshal(&conf)
	if err != nil {
		return fmt.Errorf("unable to parse toml file %s: %s", filePath, err)
	}

	InfoConf = &conf

	return nil
}

// Display config
func (c *InfoConfig) Display() {
	log.Println("Server:", c.Server)
	log.Println("Data:", c.Data)
	log.Println("Clients:", c.Clients)
	log.Println("Stats:", c.Stats)
	log.Println("CPU:", c.CPU)
	log.Println("Replication:", c.Replication)
	log.Println("Keyspace:", c.Keyspace)
	log.Println("Execcount:", c.Execcount)
	log.Println("Commandstats:", c.Commandstats)
	log.Println("Rocksdb:", c.Rocksdb)
	log.Println("Info:", c.Info)
	log.Println("InfoAll:", c.InfoAll)
}

func (c *InfoConfig) CheckInfo() {
	if c.Server && c.Data && c.Clients && c.Stats && c.CPU && c.Replication && c.Keyspace {
		c.Info = true
		if c.Execcount && c.Commandstats && c.Rocksdb {
			c.InfoAll = true
		} else {
			c.InfoAll = false
		}
	} else {
		c.InfoAll = false
		c.Info = false
	}
}
