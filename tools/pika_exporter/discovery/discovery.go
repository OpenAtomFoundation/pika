package discovery

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	defaultSeparator = ","
)

type Instance struct {
	Addr     string
	Password string
	Alias    string
}

type Discovery interface {
	GetInstances() []Instance
}

type cmdArgsDiscovery struct {
	instances []Instance
}

func NewCmdArgsDiscovery(addr, password, alias string) (*cmdArgsDiscovery, error) {
	if addr == "" {
		addr = "localhost:9221"
	}
	addrs := strings.Split(addr, defaultSeparator)
	passwords := strings.Split(password, defaultSeparator)
	for len(passwords) < len(addrs) {
		passwords = append(passwords, passwords[0])
	}
	aliases := strings.Split(alias, defaultSeparator)
	for len(aliases) < len(addrs) {
		aliases = append(aliases, aliases[0])
	}

	instances := make([]Instance, len(addrs))
	for i := range addrs {
		instances[i] = Instance{
			Addr:     addrs[i],
			Password: passwords[i],
			Alias:    aliases[i],
		}
	}
	return &cmdArgsDiscovery{instances: instances}, nil
}

func (d *cmdArgsDiscovery) GetInstances() []Instance {
	return d.instances
}

type fileDiscovery struct {
	instances []Instance
}

func NewFileDiscovery(fileName string) (*fileDiscovery, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	var instances []Instance
	for _, record := range records {
		instance := Instance{}
		length := len(record)
		switch length {
		case 3:
			instance.Addr = record[0]
			instance.Password = record[1]
			instance.Alias = record[2]
		case 2:
			instance.Addr = record[0]
			instance.Password = record[1]
		case 1:
			instance.Addr = record[0]
		default:
			log.Warnln("pika hosts file has invalid data:", record)
			continue
		}

		instances = append(instances, instance)
	}

	return &fileDiscovery{instances: instances}, nil
}

func (d *fileDiscovery) GetInstances() []Instance {
	return d.instances
}

type codisDiscovery struct {
	instances []Instance
}

func NewCodisDiscovery(url, password, alias string) (*codisDiscovery, error) {
	resp, err := http.Get(url)
	if err != nil {
		log.Warnln("Get codis topom failed:", err)
		return nil, err
	}
	defer resp.Body.Close()

	var result CodisTopomInfo
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Warnln("Response body decode failed:", err)
		return &codisDiscovery{}, err
	}

	var addrs []string
	for _, model := range result.Stats.Group.Models {
		for _, server := range model.Servers {
			if server.Server != "" {
				addrs = append(addrs, server.Server)
			}
		}
	}

	passwords := strings.Split(password, defaultSeparator)
	for len(passwords) < len(addrs) {
		passwords = append(passwords, passwords[0])
	}

	aliases := strings.Split(alias, defaultSeparator)
	for len(aliases) < len(addrs) {
		aliases = append(aliases, aliases[0])
	}

	instances := make([]Instance, len(addrs))
	for i := range addrs {
		instances[i] = Instance{
			Addr:     addrs[i],
			Password: passwords[i],
			Alias:    aliases[i],
		}
	}
	return &codisDiscovery{instances: instances}, nil
}

func (d *codisDiscovery) GetInstances() []Instance {
	return d.instances
}
