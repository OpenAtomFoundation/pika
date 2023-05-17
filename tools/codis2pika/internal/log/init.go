package log

import (
	"fmt"
	"os"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/config"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func Init() {

	// log level
	switch config.Config.Advanced.LogLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		panic(fmt.Sprintf("unknown log level: %s", config.Config.Advanced.LogLevel))
	}

	// log file
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	fileWriter, err := os.OpenFile(config.Config.Advanced.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("open log file failed: %s", err))
	}
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	logger = zerolog.New(multi).With().Timestamp().Logger()
}
