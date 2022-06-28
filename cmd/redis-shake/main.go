package main

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/commands"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/filter"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/reader"
	"github.com/alibaba/RedisShake/internal/writer"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Println("Usage: redis-shake <config file> <lua file>")
		fmt.Println("Example: redis-shake config.toml lua.lua")
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
	if len(os.Args) == 2 {
		log.Infof("No lua file specified, will not filter any cmd.")
	}

	// create writer
	var theWriter writer.Writer
	switch config.Config.Target.Type {
	case "standalone":
		if len(config.Config.Target.Addresses) != 1 {
			log.Panicf("standalone target must have only one address")
		}
		theWriter = writer.NewRedisWriter(config.Config.Target.Addresses[0], config.Config.Target.Password, config.Config.Target.IsTLS)
	case "cluster":
		if len(config.Config.Target.Addresses) == 1 {
			log.Panicf("cluster target must have at least two address")
		}
		theWriter = writer.NewRedisClusterWriter(config.Config.Target.Addresses, config.Config.Target.Password, config.Config.Target.IsTLS)
	default:
		log.Panicf("unknown target type: %s", config.Config.Target.Type)
	}

	// create reader
	source := &config.Config.Source
	theReader := reader.NewPSyncReader(source.Address, source.Password, source.IsTLS)
	ch := theReader.StartRead()

	id := uint64(0)
	for e := range ch {
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)

		// filter
		code := filter.Filter(e)
		if code == filter.Allow {
			theWriter.Write(e)
		} else if code == filter.Disallow {
			// do something
			continue
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.ToString())
		}
	}
}
