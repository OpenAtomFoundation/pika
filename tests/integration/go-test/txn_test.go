package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
)

const PORT = 9221

var txnClient *redis.Client
var cmdClient *redis.Client
var ctx context.Context
var txnCost time.Duration
var cmdCost time.Duration

func init() {
	txnClient = redis.NewClient(&redis.Options{
		Addr:         "localhost:" + strconv.Itoa(PORT),
		DB:           0,
		IdleTimeout:  time.Duration(120),
		WriteTimeout: time.Second * 100,
		ReadTimeout:  time.Second * 100,
	})
	cmdClient = redis.NewClient(&redis.Options{
		Addr:         "localhost:" + strconv.Itoa(PORT),
		DB:           0,
		IdleTimeout:  time.Duration(120),
		WriteTimeout: time.Second * 100,
		ReadTimeout:  time.Second * 100,
	})
	ctx = context.Background()
}

func AssertEqualRedisString(t *testing.T, expected, result string) {
	if expected == "" {
		assert.Equal(t, "get key: redis: nil", result)
	} else {
		assert.Equal(t, "get key: "+expected, result)
	}
}

/**
测试执行在事务中执行命令时不阻塞其他普通命令的执行
*/
func TestTxnNoBlock(t *testing.T) {
	pipe := txnClient.TxPipeline()
	pipe.Get(ctx, "key")
	pipe.Set(ctx, "key", "value", 0)
	for i := 0; i < 9999; i++ {
		pipe.Set(ctx, "key", "value", 0)
	}
	pipe.LPushX(ctx, "aaa", "xxx")
	resultChann := make(chan []redis.Cmder)
	go func(txnCost *time.Duration) {
		start := time.Now()
		res, _ := pipe.Exec(ctx)
		*txnCost = time.Since(start)
		fmt.Println("exec cmd duration:", *txnCost)
		resultChann <- res
	}(&txnCost)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(cmdCost *time.Duration) {
		time.Sleep(time.Millisecond * 5)
		start := time.Now()
		cmdClient.Set(ctx, "keyaa", "value", 0)
		*cmdCost = time.Since(start)
		fmt.Println("other cmd duration:", *cmdCost)
		wg.Done()
	}(&cmdCost)
	<-resultChann
	wg.Wait()
	assert.Equal(t, true, cmdCost < (txnCost/10))
}

func TestWatch1(t *testing.T) {
	txnClient.Watch(ctx, func(tx *redis.Tx) error { // 这个func相当于就是被一对watch和unwatch所包含了
		pipe := tx.TxPipeline()
		cmdClient.Set(ctx, "key", "1", 0)
		pipe.Set(ctx, "key", "2", 0)
		pipe.Get(ctx, "key")
		_, err := pipe.Exec(ctx)
		assert.NotNil(t, err)
		return nil
	}, "key")
}

func TestWatch2(t *testing.T) {
	cmdClient.Set(ctx, "key", "1", 0)
	txnClient.Watch(ctx, func(tx *redis.Tx) error {
		pipe := tx.TxPipeline()
		pipe.Select(ctx, 1)
		pipe.FlushDB(ctx)
		pipe.Get(ctx, "key")
		pipe.Select(ctx, 0)
		pipe.Get(ctx, "key")
		results, _ := pipe.Exec(ctx)
		AssertEqualRedisString(t, "", results[2].String())
		AssertEqualRedisString(t, "1", results[4].String())
		return nil
	}, "key")
}

// 在事务中有另一个事务来使用flushdb清除db1的数据，不会影响到watch的这个db的key的事务执行
func TestWatch3(t *testing.T) {
	watchKey := "key"
	watchkeyValue := "value"
	cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	txnClient.Watch(ctx, func(tx *redis.Tx) error {
		cmdClient.Watch(ctx, func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()
			pipe.Select(ctx, 1)
			pipe.FlushDB(ctx)
			pipe.Exec(ctx)
			return nil
		}, watchKey)
		pipe := tx.TxPipeline()
		pipe.Get(ctx, watchKey)
		results, _ := pipe.Exec(ctx)
		AssertEqualRedisString(t, watchkeyValue, results[0].String())
		return nil
	}, watchKey)
}

// 测试flushall命令会使watch的key失败
func TestWatch4(t *testing.T) {
	watchKey := "key"
	watchkeyValue := "value"
	cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	txnClient.Watch(ctx, func(tx *redis.Tx) error {
		cmdClient.Watch(ctx, func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()
			pipe.Select(ctx, 1)
			pipe.FlushAll(ctx)
			pipe.Exec(ctx)
			return nil
		}, watchKey)
		pipe := tx.TxPipeline()
		pipe.Get(ctx, watchKey)
		_, err := pipe.Exec(ctx)
		assert.NotNil(t, err)
		return nil
	}, watchKey)
}

// 测试watch的key有多个类型
func TestWatch5(t *testing.T) {
	watchKey := "key"
	watchkeyValue := "value"
	status := cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	assert.Nil(t, status.Err())
	intCmd := cmdClient.LPush(ctx, watchKey, watchkeyValue, watchkeyValue)
	assert.Nil(t, intCmd.Err())
	err := txnClient.Watch(ctx, func(tx *redis.Tx) error {
		return nil
	}, watchKey)
	assert.NotNil(t, err)
}

// 测试select命令
func TestSelectInTxn(t *testing.T) {
	watchKey := "key"
	noExist := "noExist"
	watchkeyValue := "value"
	modifiedValue := "modified"
	status := cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	assert.Nil(t, status.Err())
	intCmd := cmdClient.LPush(ctx, watchKey, watchkeyValue, watchkeyValue)
	assert.Nil(t, intCmd.Err())

	err := txnClient.Watch(ctx, func(tx *redis.Tx) error {
		tx.Select(ctx, 1) // 这个是和txnClient.Watch使用的一个端口
		tx.Watch(ctx, watchKey)
		cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
		pipeline := tx.TxPipeline()
		pipeline.Set(ctx, watchKey, modifiedValue, 0)
		pipeline.Get(ctx, watchKey)
		cmders, _ := pipeline.Exec(ctx) // 这个也是和txnClient.Watch使用的一个端口
		AssertEqualRedisString(t, modifiedValue, cmders[1].String())
		return nil
	}, noExist)
	assert.Nil(t, err)
}

func TestDiscard(t *testing.T) {
	watchKey := "key"
	watchkeyValue := "value"
	cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	pipeline := cmdClient.TxPipeline()
	pipeline.Set(ctx, watchKey, "modify", 0)
	pipeline.Discard()
	stringCmd := cmdClient.Get(ctx, watchKey)
	AssertEqualRedisString(t, watchkeyValue, stringCmd.String())
}

// 测试unwatch的基本功能，unwatch之后事务应该不受影响
func TestUnwatch1(t *testing.T) {
	watchKey := "key"
	watchkeyValue := "value"
	modifiedValue := "modified"
	cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
	txnClient.Watch(ctx, func(tx *redis.Tx) error {
		pipeline := tx.TxPipeline()
		cmdClient.Set(ctx, watchKey, modifiedValue, 0)
		tx.Unwatch(ctx)
		pipeline.Get(ctx, watchKey)
		cmders, _ := pipeline.Exec(ctx)
		AssertEqualRedisString(t, modifiedValue, cmders[0].String())
		return nil
	}, watchKey)
}
