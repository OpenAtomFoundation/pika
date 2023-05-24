package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	_ "net/http/pprof"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/commands"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/config"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/filter"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/reader"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/statistics"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/writer"
)

func main() {

	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Println("Usage: codis2pika <config file> <lua file>")
		fmt.Println("Example: codis2pika config.toml lua.lua")
		os.Exit(1)
	}

	if len(os.Args) == 3 {
		luaFile := os.Args[2]
		filter.LoadFromFile(luaFile)
	}

	// load config
	configFile := os.Args[1]
	config.LoadFromFile(configFile)

	log.Init()
	log.Infof("GOOS: %s, GOARCH: %s", runtime.GOOS, runtime.GOARCH)
	log.Infof("Ncpu: %d, GOMAXPROCS: %d", config.Config.Advanced.Ncpu, runtime.GOMAXPROCS(0))
	log.Infof("pid: %d", os.Getpid())
	log.Infof("pprof_port: %d", config.Config.Advanced.PprofPort)
	if len(os.Args) == 2 {
		log.Infof("No lua file specified, will not filter any cmd.")
	}
	//性能检测
	if config.Config.Advanced.PprofPort != 0 {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.PprofPort), nil)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// create writer

	var theWriter writer.Writer
	target := &config.Config.Target
	switch config.Config.Target.Type {
	//根据模式不同，使用不同的写方法
	case "standalone":
		fmt.Println("create NewRedisWriter: ")
		theWriter = writer.NewRedisWriter(target.Address, target.Username, target.Password, target.IsTLS)
	case "cluster":
		fmt.Println("This version does not support targeting clusters. Please customize the cluster allocation algorithm.")
		return
		// clusters := config.Config.Cluster
		// theWriter = writer.NewRedisClusterWriter(target.Address, target.Username, target.Password, target.IsTLS, clusters)
	default:
		log.Panicf("unknown target type: %s", target.Type)
	}

	// create reader
	source := &config.Config.Source
	var theReader reader.Reader
	if source.Type == "sync" {
		fmt.Println("create reader NewPSyncReader: ")
		theReader = reader.NewPSyncReader(source.Address, source.Username, source.Password, source.IsTLS, source.ElastiCachePSync)
	} else {
		log.Panicf("unknown source type: %s", source.Type)
	}
	ch := theReader.StartRead()

	// start sync
	fmt.Println("start sync: ")
	statistics.Init()
	id := uint64(0)

	for e := range ch {
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)

		// filter
		code := filter.Filter(e)
		if code == filter.Allow {

			theWriter.Write(e)
			statistics.AddAllowEntriesCount()
		} else if code == filter.Disallow {
			// do something
			statistics.AddDisallowEntriesCount()
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.ToString())
		}

	}
	//wg.Wait()
	log.Infof("finished.")
}
