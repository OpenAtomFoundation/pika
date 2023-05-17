package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/pelletier/go-toml/v2"
)

type tomlSource struct {
	Type             string `toml:"type"`
	Address          string `toml:"address"`
	Username         string `toml:"username"`
	Password         string `toml:"password"`
	IsTLS            bool   `toml:"tls"`
	ElastiCachePSync string `toml:"elasticache_psync"`
	RDBFilePath      string `toml:"rdb_file_path"`
}

type tomlTarget struct {
	Type     string `toml:"type"`
	Username string `toml:"username"`
	Address  string `toml:"address"`
	Password string `toml:"password"`
	IsTLS    bool   `toml:"tls"`
}

type tomlAdvanced struct {
	Dir string `toml:"dir"`

	Ncpu int `toml:"ncpu"`

	PprofPort int `toml:"pprof_port"`

	// log
	LogFile     string `toml:"log_file"`
	LogLevel    string `toml:"log_level"`
	LogInterval int    `toml:"log_interval"`

	// rdb restore
	RDBRestoreCommandBehavior string `toml:"rdb_restore_command_behavior"`

	// for writer
	PipelineCountLimit              uint64 `toml:"pipeline_count_limit"`
	TargetRedisClientMaxQuerybufLen uint64 `toml:"target_redis_client_max_querybuf_len"`
	TargetRedisProtoMaxBulkLen      uint64 `toml:"target_redis_proto_max_bulk_len"`
}

type tomlShakeConfig struct {
	Source   tomlSource
	Target   tomlTarget
	Advanced tomlAdvanced
}

var Config tomlShakeConfig

func init() {
	// source
	Config.Source.Type = "sync"
	Config.Source.Address = ""
	Config.Source.Username = ""
	Config.Source.Password = ""
	Config.Source.IsTLS = false
	Config.Source.ElastiCachePSync = ""

	// target
	Config.Target.Type = "standalone"
	Config.Target.Address = ""
	Config.Target.Username = ""
	Config.Target.Password = ""
	Config.Target.IsTLS = false

	// advanced
	Config.Advanced.Dir = "data"
	Config.Advanced.Ncpu = 4
	Config.Advanced.PprofPort = 0
	Config.Advanced.LogFile = "codis2pika.log"
	Config.Advanced.LogLevel = "info"
	Config.Advanced.LogInterval = 5
	Config.Advanced.RDBRestoreCommandBehavior = "rewrite"
	Config.Advanced.PipelineCountLimit = 1024
	Config.Advanced.TargetRedisClientMaxQuerybufLen = 1024 * 1000 * 1000
	Config.Advanced.TargetRedisProtoMaxBulkLen = 512 * 1000 * 1000

}

func LoadFromFile(filename string) {

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err.Error())
	}

	decoder := toml.NewDecoder(bytes.NewReader(buf))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&Config)
	if err != nil {
		missingError, ok := err.(*toml.StrictMissingError)
		if ok {
			panic(fmt.Sprintf("decode config error:\n%s", missingError.String()))
		}
		panic(err.Error())
	}

	// dir
	err = os.MkdirAll(Config.Advanced.Dir, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}
	err = os.Chdir(Config.Advanced.Dir)
	if err != nil {
		panic(err.Error())
	}

	// cpu core
	var ncpu int
	if Config.Advanced.Ncpu == 0 {
		ncpu = runtime.NumCPU()
	} else {
		ncpu = Config.Advanced.Ncpu
	}
	runtime.GOMAXPROCS(ncpu)
}
