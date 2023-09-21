package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"strings"
	"sync"
	"time"
)

func AssertEqualRedisString(expected string, result redis.Cmder) {
	if expected == "" {
		Expect(strings.HasSuffix(result.String(), "nil")).To(BeTrue())
	} else {
		if !strings.HasSuffix(result.String(), expected) {
			Expect(expected).To(BeEquivalentTo(result.String()))
		}
	}
}

var _ = Describe("Text Txn", func() {
	ctx := context.TODO()
	var txnClient *redis.Client
	var cmdClient *redis.Client
	var txnCost time.Duration
	var cmdCost time.Duration

	BeforeEach(func() {
		txnClient = redis.NewClient(pikaOptions1())
		cmdClient = redis.NewClient(pikaOptions1())
	})
	Describe("test watch", func() {
		It("basic watch", func() {
			txnClient.Watch(ctx, func(tx *redis.Tx) error { // 这个func相当于就是被一对watch和unwatch所包含了
				pipe := tx.TxPipeline()
				cmdClient.Set(ctx, "key", "1", 0)
				pipe.Set(ctx, "key", "2", 0)
				pipe.Get(ctx, "key")
				_, err := pipe.Exec(ctx)
				Expect(err).To(HaveOccurred())
				return nil
			}, "key")
		})
		It("txn failed cause watch", func() {
			setRes := cmdClient.Set(ctx, "key", "1", 0)
			Expect(setRes.Err()).NotTo(HaveOccurred())
			txnClient.Watch(ctx, func(tx *redis.Tx) error {
				pipe := tx.TxPipeline()

				selectRes := pipe.Select(ctx, 1)
				Expect(selectRes.Err()).NotTo(HaveOccurred())
				pipe.FlushDB(ctx)
				pipe.Get(ctx, "key")
				pipe.Select(ctx, 0)
				pipe.Get(ctx, "key")
				results, _ := pipe.Exec(ctx)
				AssertEqualRedisString("", results[2])
				AssertEqualRedisString("1", results[4])
				return nil
			}, "key")
		})
		// 在事务中有另一个事务来使用flushdb清除db1的数据，不会影响到watch的这个db的key的事务执行
		It("test watch1", func() {
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
				AssertEqualRedisString(watchkeyValue, results[0])
				return nil
			}, watchKey)
		})
		// 测试watch的key有多个类型
		It("test watch multi type key", func() {
			watchKey := "key"
			watchkeyValue := "value"
			status := cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
			Expect(status.Err()).NotTo(HaveOccurred())
			intCmd := cmdClient.LPush(ctx, watchKey, watchkeyValue, watchkeyValue)
			Expect(intCmd.Err()).NotTo(HaveOccurred())
			err := txnClient.Watch(ctx, func(tx *redis.Tx) error {
				return nil
			}, watchKey)
			Expect(err).To(HaveOccurred())
		})
		//// 测试flushall命令会使watch的key失败
		It("txn failed cause of flushall", func() {
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
				Expect(err).To(HaveOccurred())
				return nil
			}, watchKey)
		})
		// 测试select命令
		It("select in txn", func() {
			watchKey := "key"
			noExist := "noExist"
			watchkeyValue := "value"
			modifiedValue := "modified"
			status := cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
			Expect(status.Err()).NotTo(HaveOccurred())
			intCmd := cmdClient.LPush(ctx, watchKey, watchkeyValue, watchkeyValue)
			Expect(intCmd.Err()).NotTo(HaveOccurred())

			err := txnClient.Watch(ctx, func(tx *redis.Tx) error {
				tx.Select(ctx, 1) // 这个是和txnClient.Watch使用的一个端口
				tx.Watch(ctx, watchKey)
				cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
				pipeline := tx.TxPipeline()
				pipeline.Set(ctx, watchKey, modifiedValue, 0)
				pipeline.Get(ctx, watchKey)
				cmders, _ := pipeline.Exec(ctx) // 这个也是和txnClient.Watch使用的一个端口
				AssertEqualRedisString(modifiedValue, cmders[1])
				return nil
			}, noExist)
			Expect(err).NotTo(HaveOccurred())
		})

		// 测试执行在事务中执行命令时不阻塞其他普通命令的执行
		It("test txn no block other cmd", func() {
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
				resultChann <- res
			}(&txnCost)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func(cmdCost *time.Duration) {
				time.Sleep(time.Millisecond * 5)
				start := time.Now()
				cmdClient.Set(ctx, "keyaa", "value", 0)
				*cmdCost = time.Since(start)
				wg.Done()
			}(&cmdCost)
			<-resultChann
			wg.Wait()
			Expect(cmdCost < (txnCost / 10)).To(BeTrue())
		})
	})

	Describe("Test Discard", func() {
		It("test discard", func() {
			watchKey := "key"
			watchkeyValue := "value"
			cmdClient.Set(ctx, watchKey, watchkeyValue, 0)
			pipeline := cmdClient.TxPipeline()
			pipeline.Set(ctx, watchKey, "modify", 0)
			pipeline.Discard()
			stringCmd := cmdClient.Get(ctx, watchKey)
			AssertEqualRedisString(watchkeyValue, stringCmd)
		})
	})
	Describe("Test Unwatch", func() {
		// 测试unwatch的基本功能，unwatch之后事务应该不受影响
		It("unwatch1", func() {
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
				AssertEqualRedisString(modifiedValue, cmders[0])
				return nil
			}, watchKey)
		})
	})
	Describe("Test Blpop In Txn", func() {
		It("blpop1", func() {
			listKey := "key"
			listValue := "v1"
			cmdClient.Del(ctx, listKey)
			go func() {
				cmdClient.BLPop(ctx, 0, listKey)
			}()
			time.After(time.Duration(1))
			pipeline := txnClient.TxPipeline()
			pipeline.LPush(ctx, listKey, listValue)
			pipeline.LPop(ctx, listKey)
			result, err := pipeline.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			AssertEqualRedisString(listValue, result[1])
		})

		It("blpop2", func() {
			listKey := "key"
			setKey := "setkey"
			setValue := "setValue"
			txnClient.Del(ctx, listKey)
			txPipeline := txnClient.TxPipeline()
			txPipeline.BLPop(ctx, 0, listKey)
			txPipeline.Set(ctx, setKey, setValue, 0)
			txPipeline.Get(ctx, setKey)
			cmders, _ := txPipeline.Exec(ctx)
			AssertEqualRedisString(setValue, cmders[2])
		})

	})
	// Because when there is only one list result, Redis returns in two cases, one with * and one without * ,
	// but go-redis knows only the ones without *
	// pika return pop result with *
	Describe("Test Blpop", func() {
		It("blpop1", func() {
			listKey := "key"
			listValue := "v1"
			cmdClient.Del(ctx, listKey)
			cmdClient.LPush(ctx, listKey, listValue)
			popCmd := cmdClient.LPop(ctx, listKey)
			Expect(strings.HasSuffix(popCmd.String(), listValue)).To(BeTrue())
		})
	})

	AfterEach(func() {
		Expect(txnClient.Close()).NotTo(HaveOccurred())
		Expect(cmdClient.Close()).NotTo(HaveOccurred())
	})
})
