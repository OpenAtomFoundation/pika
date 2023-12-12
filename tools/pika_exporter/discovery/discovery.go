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

type InstanceProxy struct {
	ID          int
	Addr        string
	ProductName string
}

type Discovery interface {
	GetInstances() []Instance
	GetInstancesProxy() []InstanceProxy
	CheckUpdate(chan int, string)
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

func (d *cmdArgsDiscovery) GetInstancesProxy() []InstanceProxy {
	return nil
}

func (d *cmdArgsDiscovery) CheckUpdate(chan int, string) {}

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

func (d *fileDiscovery) GetInstancesProxy() []InstanceProxy {
	return nil
}

func (d *fileDiscovery) CheckUpdate(chan int, string) {}

type codisDiscovery struct {
	instances     []Instance
	instanceProxy []InstanceProxy
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

	instancesproxy := make([]InstanceProxy, len(result.Stats.Proxy.Models))
	for i := range result.Stats.Proxy.Models {
		instancesproxy[i] = InstanceProxy{
			ID:          result.Stats.Proxy.Models[i].Id,
			Addr:        result.Stats.Proxy.Models[i].AdminAddr,
			ProductName: result.Stats.Proxy.Models[i].ProductName,
		}
	}
	return &codisDiscovery{
		instances:     instances,
		instanceProxy: instancesproxy,
	}, nil
}

func (d *codisDiscovery) GetInstances() []Instance {
	return d.instances
}

func (d *codisDiscovery) GetInstancesProxy() []InstanceProxy {
	return d.instanceProxy
}

func (d *codisDiscovery) CheckUpdate(updatechan chan int, codisaddr string) {
	newdis, err := NewCodisDiscovery(codisaddr, "", "")
	if err != nil {
		log.Fatalln(" failed. err:", err)
	}
	diff := d.comparedis(newdis)
	if !diff {
		updatechan <- 1
	}
}

func (d *codisDiscovery) comparedis(new_instance *codisDiscovery) bool {
	var addrs, addrsProxy []string
	var diff bool = false
	for _, instance := range new_instance.instances {
		addrs = append(addrs, instance.Addr)
	}
	for _, instance := range d.instances {
		if !contains(instance.Addr, addrs) {
			diff = true
			return false
		}
	}
	for _, instance := range new_instance.instanceProxy {
		addrsProxy = append(addrsProxy, instance.Addr)
	}
	for _, instance := range d.instanceProxy {
		if !contains(instance.Addr, addrsProxy) {
			diff = true
			return false
		}
	}
	if !diff && len(new_instance.instances) == len(d.instances) && len(new_instance.instanceProxy) == len(d.instanceProxy) {
		return true
	}
	return false
}

func contains(addr string, addrs []string) bool {
	for _, a := range addrs {
		if a == addr {
			return true
		}
	}
	return false
}
