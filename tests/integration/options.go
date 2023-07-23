package pika_integration

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type TimeValue struct {
	time.Time
}

func (t *TimeValue) ScanRedis(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

func pikaOptions1() *redis.Options {
	return &redis.Options{
		Addr:         "127.0.0.1:9221",
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	}
}

func pikaOptions2() *redis.Options {
	return &redis.Options{
		Addr:         "127.0.0.1:9231",
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	}
}
