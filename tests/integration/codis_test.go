package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sort"
	"time"
)

var _ = Describe("List Commands Codis", func() {
	ctx := context.TODO()
	client := redis.NewClient(PikaOption(CODISADDR))

	BeforeEach(func() {

	})

	AfterEach(func() {
		//Expect(client.Close()).NotTo(HaveOccurred())
	})

	FDescribe("lists", func() {
		It("should LIndex", func() {
			key := uuid.New().String()
			lPush := client.LPush(ctx, key, "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, key, "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lIndex := client.LIndex(ctx, key, 0)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("Hello"))

			lIndex = client.LIndex(ctx, key, -1)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("World"))

			lIndex = client.LIndex(ctx, key, 3)
			Expect(lIndex.Err()).To(Equal(redis.Nil))
			Expect(lIndex.Val()).To(Equal(""))
		})

		It("should LInsert", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert(ctx, key, "BEFORE", "World", "There")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "There", "World"}))
		})

		//It("should LMPop", func() {
		//	err := client.LPush(ctx, "list1", "one", "two", "three", "four", "five").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	err = client.LPush(ctx, "list2", "a", "b", "c", "d", "e").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	key, val, err := client.LMPop(ctx, "left", 3, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list1"))
		//	Expect(val).To(Equal([]string{"five", "four", "three"}))
		//
		//	key, val, err = client.LMPop(ctx, "right", 3, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list1"))
		//	Expect(val).To(Equal([]string{"one", "two"}))
		//
		//	key, val, err = client.LMPop(ctx, "left", 1, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list2"))
		//	Expect(val).To(Equal([]string{"e"}))
		//
		//	key, val, err = client.LMPop(ctx, "right", 10, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list2"))
		//	Expect(val).To(Equal([]string{"a", "b", "c", "d"}))
		//
		//	err = client.LMPop(ctx, "left", 10, "list1", "list2").Err()
		//	Expect(err).To(Equal(redis.Nil))
		//
		//	err = client.Set(ctx, "list3", 1024, 0).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	err = client.LMPop(ctx, "left", 10, "list1", "list2", "list3").Err()
		//	Expect(err.Error()).To(Equal("WRONGTYPE Operation against a key holding the wrong kind of value"))
		//
		//	err = client.LMPop(ctx, "right", 0, "list1", "list2").Err()
		//	Expect(err).To(HaveOccurred())
		//})

		//It("should BLMPop", func() {
		//	err := client.LPush(ctx, "list1", "one", "two", "three", "four", "five").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	err = client.LPush(ctx, "list2", "a", "b", "c", "d", "e").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	key, val, err := client.BLMPop(ctx, 0, "left", 3, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list1"))
		//	Expect(val).To(Equal([]string{"five", "four", "three"}))
		//
		//	key, val, err = client.BLMPop(ctx, 0, "right", 3, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list1"))
		//	Expect(val).To(Equal([]string{"one", "two"}))
		//
		//	key, val, err = client.BLMPop(ctx, 0, "left", 1, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list2"))
		//	Expect(val).To(Equal([]string{"e"}))
		//
		//	key, val, err = client.BLMPop(ctx, 0, "right", 10, "list1", "list2").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(key).To(Equal("list2"))
		//	Expect(val).To(Equal([]string{"a", "b", "c", "d"}))
		//
		//})
		//
		//It("should BLMPopBlocks", func() {
		//	started := make(chan bool)
		//	done := make(chan bool)
		//	go func() {
		//		defer GinkgoRecover()
		//
		//		started <- true
		//		key, val, err := client.BLMPop(ctx, 0, "left", 1, "list_list").Result()
		//		Expect(err).NotTo(HaveOccurred())
		//		Expect(key).To(Equal("list_list"))
		//		Expect(val).To(Equal([]string{"a"}))
		//		done <- true
		//	}()
		//	<-started
		//
		//	select {
		//	case <-done:
		//		Fail("BLMPop is not blocked")
		//	case <-time.After(time.Second):
		//		//ok
		//	}
		//
		//	_, err := client.LPush(ctx, "list_list", "a").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	select {
		//	case <-done:
		//		//ok
		//	case <-time.After(time.Second):
		//		Fail("BLMPop is still blocked")
		//	}
		//})

		//It("should BLMPop timeout", func() {
		//	_, val, err := client.BLMPop(ctx, time.Second, "left", 1, "list1").Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(BeNil())
		//
		//	Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		//
		//	stats := client.PoolStats()
		//	Expect(stats.Hits).To(Equal(uint32(2)))
		//	Expect(stats.Misses).To(Equal(uint32(1)))
		//	Expect(stats.Timeouts).To(Equal(uint32(0)))
		//})

		It("should LLen", func() {
			key := uuid.New().String()
			lPush := client.LPush(ctx, key, "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, key, "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lLen := client.LLen(ctx, key)
			Expect(lLen.Err()).NotTo(HaveOccurred())
			Expect(lLen.Val()).To(Equal(int64(2)))
		})

		// todo fix: https://github.com/OpenAtomFoundation/pika/issues/1791

		It("should LPopCount", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPopCount := client.LPopCount(ctx, key, 2)
			Expect(lPopCount.Err()).NotTo(HaveOccurred())
			Expect(lPopCount.Val()).To(Equal([]string{"one", "two"}))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three", "four"}))
		})

		//It("should LPos", func() {
		//	rPush := client.RPush(ctx, key, "a")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "c")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//
		//	lPos := client.LPos(ctx, key, "b", redis.LPosArgs{})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(1)))
		//
		//	lPos = client.LPos(ctx, key, "b", redis.LPosArgs{Rank: 2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(3)))
		//
		//	lPos = client.LPos(ctx, key, "b", redis.LPosArgs{Rank: -2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(1)))
		//
		//	lPos = client.LPos(ctx, key, "b", redis.LPosArgs{Rank: 2, MaxLen: 1})
		//	Expect(lPos.Err()).To(Equal(redis.Nil))
		//
		//	lPos = client.LPos(ctx, key, "z", redis.LPosArgs{})
		//	Expect(lPos.Err()).To(Equal(redis.Nil))
		//})

		//It("should LPosCount", func() {
		//	rPush := client.RPush(ctx, key, "a")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "c")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, key, "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//
		//	lPos := client.LPosCount(ctx, key, "b", 2, redis.LPosArgs{})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{1, 3}))
		//
		//	lPos = client.LPosCount(ctx, key, "b", 2, redis.LPosArgs{Rank: 2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{3}))
		//
		//	lPos = client.LPosCount(ctx, key, "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 1})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{}))
		//
		//	lPos = client.LPosCount(ctx, key, "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 0})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{1}))
		//})

		It("should LPush", func() {
			key := uuid.New().String()
			lPush := client.LPush(ctx, key, "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, key, "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should LPushX", func() {
			key := uuid.New().String()
			lPush := client.LPush(ctx, key, "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lPushX := client.LPushX(ctx, key, "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(2)))

			key2 := uuid.New().String()
			lPush = client.LPush(ctx, key2, "three")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			Expect(lPush.Val()).To(Equal(int64(1)))

			lPushX = client.LPushX(ctx, key2, "two", "one")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(3)))

			key3 := uuid.New().String()
			lPushX = client.LPushX(ctx, key3, "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, key2, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, key3, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRange", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, key, 0, 0)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one"}))

			lRange = client.LRange(ctx, key, -3, 2)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, key, -100, 100)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, key, 5, 10)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRem", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "key")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRem := client.LRem(ctx, key, -2, "hello")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"hello", "key"}))
		})

		It("should LRem binary", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "\x00\xa2\x00")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "\x00\x9d")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert(ctx, key, "BEFORE", "\x00\x9d", "\x00\x5f")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"\x00\xa2\x00", "\x00\x5f", "\x00\x9d"}))

			lRem := client.LRem(ctx, key, -1, "\x00\x5f")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(1)))

			lRange = client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"\x00\xa2\x00", "\x00\x9d"}))
		})

		It("should LSet", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lSet := client.LSet(ctx, key, 0, "four")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lSet = client.LSet(ctx, key, -2, "five")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"four", "five", "three"}))
		})

		It("should LTrim", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lTrim := client.LTrim(ctx, key, 1, -1)
			Expect(lTrim.Err()).NotTo(HaveOccurred())
			Expect(lTrim.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should RPopCount", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one", "two", "three", "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(4)))

			rPopCount := client.RPopCount(ctx, key, 2)
			Expect(rPopCount.Err()).NotTo(HaveOccurred())
			Expect(rPopCount.Val()).To(Equal([]string{"four", "three"}))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPopLPush", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, key, "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPopLPush := client.RPopLPush(ctx, key, "list2")
			Expect(rPopLPush.Err()).NotTo(HaveOccurred())
			Expect(rPopLPush.Val()).To(Equal("three"))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three"}))
		})

		It("should RPush", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush(ctx, key, "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should RPushX", func() {
			key := uuid.New().String()
			rPush := client.RPush(ctx, key, "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX := client.RPushX(ctx, key, "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(2)))

			key1 := uuid.New().String()
			rPush = client.RPush(ctx, key1, "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX = client.RPushX(ctx, key1, "two", "three")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(3)))

			key2 := uuid.New().String()
			rPushX = client.RPushX(ctx, key2, "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, key, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, key1, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, key2, 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

	})
})
var _ = Describe("Hash Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		if GlobalBefore != nil {
			GlobalBefore(ctx, client)
		}
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("hashes", func() {
		It("should HDel", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hDel := client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(1)))

			hDel = client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(0)))
		})

		It("should HExists", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hExists := client.HExists(ctx, "hash", "key")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(true))

			hExists = client.HExists(ctx, "hash", "key1")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(false))
		})

		It("should HGet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			hGet = client.HGet(ctx, "hash", "key1")
			Expect(hGet.Err()).To(Equal(redis.Nil))
			Expect(hGet.Val()).To(Equal(""))
		})

		It("should HGetAll", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			m, err := client.HGetAll(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("should scan", func() {
			now := time.Now()

			err := client.HMSet(ctx, "hash", "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.HGetAll(ctx, "hash")
			Expect(res.Err()).NotTo(HaveOccurred())

			type data struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time TimeValue `redis:"time"`
			}
			var d data
			Expect(res.Scan(&d)).NotTo(HaveOccurred())
			Expect(d.Time.UnixNano()).To(Equal(now.UnixNano()))
			d.Time.Time = time.Time{}
			Expect(d).To(Equal(data{
				Key1: "hello1",
				Key2: 123,
				Time: TimeValue{Time: time.Time{}},
			}))

			//type data2 struct {
			//	Key1 string    `redis:"key1"`
			//	Key2 int       `redis:"key2"`
			//	Time time.Time `redis:"time"`
			//}
			////err = client.HSet(ctx, "hash", &data2{
			////	Key1: "hello2",
			////	Key2: 200,
			////	Time: now,
			////}).Err()
			////Expect(err).NotTo(HaveOccurred())
			//
			//var d2 data2
			//err = client.HMGet(ctx, "hash", "key1", "key2", "time").Scan(&d2)
			//Expect(err).NotTo(HaveOccurred())
			//Expect(d2.Key1).To(Equal("hello2"))
			//Expect(d2.Key2).To(Equal(200))
			//Expect(d2.Time.Unix()).To(Equal(now.Unix()))
		})

		It("should HIncrBy", func() {
			hSet := client.HSet(ctx, "hash", "key", "5")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hIncrBy := client.HIncrBy(ctx, "hash", "key", 1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(6)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(5)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -10)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(-5)))
		})

		It("should HIncrByFloat", func() {
			hSet := client.HSet(ctx, "hash", "field", "10.50")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hIncrByFloat := client.HIncrByFloat(ctx, "hash", "field", 0.1)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(10.6))

			hSet = client.HSet(ctx, "hash", "field", "5.0e3")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(0)))

			hIncrByFloat = client.HIncrByFloat(ctx, "hash", "field", 2.0e2)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should HKeys", func() {
			hkeys := client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{}))

			hset := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hset.Err()).NotTo(HaveOccurred())
			hset = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hset.Err()).NotTo(HaveOccurred())

			hkeys = client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{"key1", "key2"}))
		})

		It("should HLen", func() {
			hSet := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			hSet = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hLen := client.HLen(ctx, "hash")
			Expect(hLen.Err()).NotTo(HaveOccurred())
			Expect(hLen.Val()).To(Equal(int64(2)))
		})

		It("should HMGet", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.HMGet(ctx, "hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"hello1"}))
		})

		It("should HSet", func() {
			_, err := client.Del(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())

			ok, err := client.HSet(ctx, "hash", map[string]interface{}{
				"key1": "hello1",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			ok, err = client.HSet(ctx, "hash", map[string]interface{}{
				"key2": "hello2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			v, err := client.HGet(ctx, "hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello1"))

			v, err = client.HGet(ctx, "hash", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello2"))

			keys, err := client.HKeys(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf([]string{"key1", "key2"}))
		})

		It("should HSet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
				Set6 string                 `redis:"set6,omitempty"`
			}

			// 命令格式不对：hset hash set1 val1 set2 1024 set3 2000000 set4
			//hSet = client.HSet(ctx, "hash", &set{
			//	Set1: "val1",
			//	Set2: 1024,
			//	Set3: 2 * time.Millisecond,
			//	Set4: nil,
			//	Set5: map[string]interface{}{"k1": 1},
			//})
			//Expect(hSet.Err()).NotTo(HaveOccurred())
			//Expect(hSet.Val()).To(Equal(int64(4)))

			//hMGet := client.HMGet(ctx, "hash", "set1", "set2", "set3", "set4", "set5", "set6")
			//Expect(hMGet.Err()).NotTo(HaveOccurred())
			//Expect(hMGet.Val()).To(Equal([]interface{}{
			//	"val1",
			//	"1024",
			//	strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
			//	"",
			//	nil,
			//	nil,
			//}))

			//hSet = client.HSet(ctx, "hash2", &set{
			//	Set1: "val2",
			//	Set6: "val",
			//})
			//Expect(hSet.Err()).NotTo(HaveOccurred())
			//Expect(hSet.Val()).To(Equal(int64(5)))
			//
			//hMGet = client.HMGet(ctx, "hash2", "set1", "set6")
			//Expect(hMGet.Err()).NotTo(HaveOccurred())
			//Expect(hMGet.Val()).To(Equal([]interface{}{
			//	"val2",
			//	"val",
			//}))
		})

		It("should HSetNX", func() {
			res := client.Del(ctx, "hash")
			Expect(res.Err()).NotTo(HaveOccurred())

			hSetNX := client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(true))

			hSetNX = client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(false))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))
		})

		It("should HVals", func() {
			err := client.HSet(ctx, "hash121", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash121", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.HVals(ctx, "hash121").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"hello1", "hello2"}))

			var slice []string
			err = client.HVals(ctx, "hash121").ScanSlice(&slice)
			Expect(err).NotTo(HaveOccurred())
			sort.Strings(slice)
			Expect(slice).To(Equal([]string{"hello1", "hello2"}))
		})

		It("should HSTRLEN", func() {
			hSet := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, "hash", "key1")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			length := client.Do(ctx, "hstrlen", "hash", "key1")

			Expect(length.Val()).To(Equal(int64(len("hello1"))))
		})
		//It("should HRandField", func() {
		//	err := client.HSet(ctx, "hash", "key1", "hello1").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//	err = client.HSet(ctx, "hash", "key2", "hello2").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	//v := client.HRandField(ctx, "hash", 1)
		//	//Expect(v.Err()).NotTo(HaveOccurred())
		//	//Expect(v.Val()).To(Or(Equal([]string{"key1"}), Equal([]string{"key2"})))
		//
		//	v := client.HRandField(ctx, "hash", 0)
		//	Expect(v.Err()).NotTo(HaveOccurred())
		//	Expect(v.Val()).To(HaveLen(0))
		//
		//	kv, err := client.HRandFieldWithValues(ctx, "hash", 1).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(kv).To(Or(
		//		Equal([]redis.KeyValue{{Key: "key1", Value: "hello1"}}),
		//		Equal([]redis.KeyValue{{Key: "key2", Value: "hello2"}}),
		//	))
		//})
	})
})
