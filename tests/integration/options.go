package pika_integration

import (
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	LOCALHOST  = "127.0.0.1"
	SLAVEPORT  = "9231"
	MASTERPORT = "9241"
	SINGLEADDR = "127.0.0.1:9221"
	SLAVEADDR  = "127.0.0.1:9231"
	MASTERADDR = "127.0.0.1:9241"
	RenameADDR = "127.0.0.1:9251"

	CODISADDR = "127.0.0.1:19000"

	ACLADDR_1 = "127.0.0.1:9261"
	ACLADDR_2 = "127.0.0.1:9271"
	ACLADDR_3 = "127.0.0.1:9281"
)

type TimeValue struct {
	time.Time
}

func (t *TimeValue) ScanRedis(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

func PikaOption(addr string) *redis.Options {
	return &redis.Options{
		Addr:         addr,
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1,
		PoolSize:     30,
		PoolTimeout:  60 * time.Second,
	}
}
