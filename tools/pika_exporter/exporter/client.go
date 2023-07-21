package exporter

import (
	"bufio"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	defaultScanCount = 100
)

const (
	keyTypeNone   = "none"
	keyTypeString = "string"
	//keyTypeHyperLogLog = "hyperloglog"
	keyTypeList = "list"
	keyTypeSet  = "set"
	keyTypeZSet = "zset"
	keyTypeHash = "hash"
)

var (
	errNotFound = errors.New("key not found")
)

type keyInfo struct {
	size    float64
	keyType string
}

type client struct {
	addr, alias string
	conn        redis.Conn
}

func newClient(addr, password, alias string) (*client, error) {
	conn, err := redis.Dial("tcp", addr,
		redis.DialConnectTimeout(5*time.Second),
		redis.DialWriteTimeout(5*time.Second),
		redis.DialReadTimeout(5*time.Second),
		redis.DialPassword(password))
	if err != nil {
		return nil, err
	}

	return &client{
		addr:  addr,
		alias: alias,
		conn:  conn,
	}, nil
}

func (c *client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *client) Addr() string {
	return c.addr
}

func (c *client) Alias() string {
	return c.alias
}

func (c *client) Select(db string) error {
	_, err := c.conn.Do("SELECT", db)
	return err
}

func (c *client) GetInfo() (string, error) {
	if InfoConf.InfoAll {
		return c.InfoAll()
	} else if InfoConf.Info {
		return c.Info()
	} else {
		return c.InfoSubCommand()
	}
}

func (c *client) Info() (string, error) {
	return redis.String(c.conn.Do("INFO"))
}

func (c *client) InfoAll() (string, error) {
	return redis.String(c.conn.Do("INFO", "ALL"))
}

func (c *client) InfoSubCommand() (string, error) {
	var rst []string

	sectionsMap := map[string]bool{
		"SERVER":             InfoConf.Server,
		"DATA":               InfoConf.Data,
		"CLIENTS":            InfoConf.Clients,
		"STATS":              InfoConf.Stats,
		"CPU":                InfoConf.CPU,
		"REPLICATION":        InfoConf.Replication,
		"KEYSPACE":           InfoConf.Keyspace,
		"COMMAND_EXEC_COUNT": InfoConf.Execcount,
		"COMMANDSTATS":       InfoConf.Commandstats,
		"ROCKSDB":            InfoConf.Rocksdb,
	}
	for section, flag := range sectionsMap {
		if flag {
			info, err := redis.String(c.conn.Do("INFO", section))
			if err == nil {
				rst = append(rst, info)
			}
		}
	}

	return strings.Join(rst, "\n"), nil
}

func (c *client) InfoKeySpaceZero() (string, error) {
	return redis.String(c.conn.Do("INFO", "KEYSPACE", 0))
}

func (c *client) InfoKeySpaceOne() (string, error) {
	return redis.String(c.conn.Do("INFO", "KEYSPACE", 1))
}

func (c *client) InstanceModeInfo() (string, error) {
	getslices, err := redis.Strings(c.conn.Do("CONFIG", "GET", "instance-mode"))
	if err != nil {
		return "", fmt.Errorf("CONFIG GET instance-mode '%s' ", err)
	}
	for _, gets := range getslices {
		scanner := bufio.NewScanner(strings.NewReader(gets))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if line == "instance-mode" {
				continue
			}
			return line, nil
		}
	}
	return "", fmt.Errorf("error retrieving CONFIG GET instance-mode '%s' ", err)
}

func (c *client) LabelConsensusLevelInfo() (string, error) {
	getslices, err := redis.Strings(c.conn.Do("CONFIG", "GET", "consensus-level"))
	if err != nil {
		return "", fmt.Errorf("CONFIG GET consensus-level '%s' ", err)
	}
	for _, gets := range getslices {
		scanner := bufio.NewScanner(strings.NewReader(gets))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if line == "consensus-level" {
				continue
			}
			return line, nil
		}
	}
	return "", fmt.Errorf("error retrieving CONFIG GET consensus-level '%s' ", err)
}

// Pika的SCAN命令，会顺序迭代当前db的快照，由于Pika允许重名五次，所以SCAN有优先输出顺序，依次为：string -> hash -> list -> zset -> set
func (c *client) Scan(keyPattern string, count int) ([]string, error) {
	if count == 0 {
		count = defaultScanCount
	}

	var (
		cursor int
		keys   []string
	)
	for {
		values, err := redis.Values(c.conn.Do("SCAN", cursor, "MATCH", keyPattern, "COUNT", count))
		if err != nil {
			return keys, fmt.Errorf("error retrieving '%s' keys", keyPattern)
		}
		if len(values) != 2 {
			return keys, fmt.Errorf("invalid response from SCAN for pattern: %s", keyPattern)
		}

		ks, _ := redis.Strings(values[1], nil)
		keys = append(keys, ks...)

		if cursor, _ = redis.Int(values[0], nil); cursor == 0 {
			break
		}
	}

	return keys, nil
}

// Pikad的TYPE命令，由于Pika允许重名五次，所以TYPE有优先输出顺序，依次为：string -> hash -> list -> zset -> set，如果这个key在string中存在，那么只输出sting，如果不存在，那么则输出hash的，依次类推
func (c *client) Type(key string) (*keyInfo, error) {
	keyType, err := redis.String(c.conn.Do("TYPE", key))
	if err != nil {
		return nil, err
	}

	info := &keyInfo{keyType: keyType}
	switch keyType {
	case keyTypeNone:
		return nil, errNotFound
	case keyTypeString:
		if size, err := redis.Int64(c.conn.Do("STRLEN", key)); err == nil {
			info.size = float64(size)
		}
	case keyTypeList:
		if size, err := redis.Int64(c.conn.Do("LLEN", key)); err == nil {
			info.size = float64(size)
		}
	case keyTypeSet:
		if size, err := redis.Int64(c.conn.Do("SCARD", key)); err == nil {
			info.size = float64(size)
		}
	case keyTypeZSet:
		if size, err := redis.Int64(c.conn.Do("ZCARD", key)); err == nil {
			info.size = float64(size)
		}
	case keyTypeHash:
		if size, err := redis.Int64(c.conn.Do("HLEN", key)); err == nil {
			info.size = float64(size)
		}
	default:
		return nil, fmt.Errorf("unknown type: %v for key: %v", info.keyType, key)
	}

	return info, nil
}

func (c *client) Get(key string) (string, error) {
	return redis.String(c.conn.Do("GET", key))
}
