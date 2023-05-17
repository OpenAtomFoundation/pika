package log

import (
	"fmt"

	"github.com/rs/zerolog"
)

func Assert(condition bool, msg string) {
	if !condition {
		Panicf("Assert failed: %s", msg)
	}
}

func Debugf(format string, args ...interface{}) {
	logFinally(logger.Debug(), format, args...)
}

func Infof(format string, args ...interface{}) {
	logFinally(logger.Info(), format, args...)
}

func Warnf(format string, args ...interface{}) {
	logFinally(logger.Warn(), format, args...)
}

func Panicf(format string, args ...interface{}) {
	logFinally(logger.Panic(), format, args...)
}

func PanicError(err error) {
	Panicf(err.Error())
}

func logFinally(event *zerolog.Event, format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	event.Msg(str)
}
