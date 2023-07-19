package exporter

import (
	"github.com/pelletier/go-toml"
	"log"
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

func LoadConfig() {
	log.Println("Configuration updated:")
	err := readConfig(InfoConfigPath)
	if err != nil {
		log.Println("Error reading configuration file, err:", err)
	}
}

func readConfig(filePath string) error {
	conf := InfoConfig{}
	tree, err := toml.LoadFile(filePath)
	if err != nil {
		return err
	}

	// Unmarshal the TOML data into the Config struct
	err = tree.Unmarshal(&conf)
	if err != nil {
		return err
	}

	InfoConf = &conf
	InfoConf.CheckInfo()
	InfoConf.Display()

	return nil
}

// Display config
func (c *InfoConfig) Display() {
	// Now you can access the configuration values
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
