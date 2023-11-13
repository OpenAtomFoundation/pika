package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("List Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(pikaOptions1())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("lists", func() {
		It("should BLPop", func() {
			rPush := client.RPush(ctx, "list1", "a", "b", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			bLPop := client.BLPop(ctx, 0, "list1", "list2")
			Expect(bLPop.Err()).NotTo(HaveOccurred())
			Expect(bLPop.Val()).To(Equal([]string{"list1", "a"}))
		})

		It("should BLPopBlocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				bLPop := client.BLPop(ctx, 0, "list")
				Expect(bLPop.Err()).NotTo(HaveOccurred())
				Expect(bLPop.Val()).To(Equal([]string{"list", "a"}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BLPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BLPop is still blocked")
			}
		})

		It("should BLPop timeout", func() {
			val, err := client.BLPop(ctx, time.Second, "list1").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(BeNil())

			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(2)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
		})

		It("should BRPop", func() {
			rPush := client.RPush(ctx, "list1", "a", "b", "c")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			bRPop := client.BRPop(ctx, 0, "list1", "list2")
			Expect(bRPop.Err()).NotTo(HaveOccurred())
			Expect(bRPop.Val()).To(Equal([]string{"list1", "c"}))
		})

		It("should BRPop blocks", func() {
			started := make(chan bool)
			done := make(chan bool)
			go func() {
				defer GinkgoRecover()

				started <- true
				brpop := client.BRPop(ctx, 0, "list")
				Expect(brpop.Err()).NotTo(HaveOccurred())
				Expect(brpop.Val()).To(Equal([]string{"list", "a"}))
				done <- true
			}()
			<-started

			select {
			case <-done:
				Fail("BRPop is not blocked")
			case <-time.After(time.Second):
				// ok
			}

			rPush := client.RPush(ctx, "list", "a")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			select {
			case <-done:
				// ok
			case <-time.After(time.Second):
				Fail("BRPop is still blocked")
				// ok
			}
		})

		//It("should BRPopLPush", func() {
		//	_, err := client.BRPopLPush(ctx, "list1", "list2", time.Second).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//
		//	err = client.RPush(ctx, "list1", "a", "b", "c").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	v, err := client.BRPopLPush(ctx, "list1", "list2", 0).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(v).To(Equal("c"))
		//})

		//It("should LCS", func() {
		//	err := client.MSet(ctx, "key1", "ohmytext", "key2", "mynewtext").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	lcs, err := client.LCS(ctx, &redis.LCSQuery{
		//		Key1: "key1",
		//		Key2: "key2",
		//	}).Result()
		//
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(lcs.MatchString).To(Equal("mytext"))
		//
		//	lcs, err = client.LCS(ctx, &redis.LCSQuery{
		//		Key1: "nonexistent_key1",
		//		Key2: "key2",
		//	}).Result()
		//
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(lcs.MatchString).To(Equal(""))
		//
		//	lcs, err = client.LCS(ctx, &redis.LCSQuery{
		//		Key1: "key1",
		//		Key2: "key2",
		//		Len:  true,
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(lcs.MatchString).To(Equal(""))
		//	Expect(lcs.Len).To(Equal(int64(6)))
		//
		//	lcs, err = client.LCS(ctx, &redis.LCSQuery{
		//		Key1: "key1",
		//		Key2: "key2",
		//		Idx:  true,
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(lcs.MatchString).To(Equal(""))
		//	Expect(lcs.Len).To(Equal(int64(6)))
		//	Expect(lcs.Matches).To(Equal([]redis.LCSMatchedPosition{
		//		{
		//			Key1:     redis.LCSPosition{Start: 4, End: 7},
		//			Key2:     redis.LCSPosition{Start: 5, End: 8},
		//			MatchLen: 0,
		//		},
		//		{
		//			Key1:     redis.LCSPosition{Start: 2, End: 3},
		//			Key2:     redis.LCSPosition{Start: 0, End: 1},
		//			MatchLen: 0,
		//		},
		//	}))
		//
		//	lcs, err = client.LCS(ctx, &redis.LCSQuery{
		//		Key1:         "key1",
		//		Key2:         "key2",
		//		Idx:          true,
		//		MinMatchLen:  3,
		//		WithMatchLen: true,
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(lcs.MatchString).To(Equal(""))
		//	Expect(lcs.Len).To(Equal(int64(6)))
		//	Expect(lcs.Matches).To(Equal([]redis.LCSMatchedPosition{
		//		{
		//			Key1:     redis.LCSPosition{Start: 4, End: 7},
		//			Key2:     redis.LCSPosition{Start: 5, End: 8},
		//			MatchLen: 4,
		//		},
		//	}))
		//
		//	_, err = client.Set(ctx, "keywithstringvalue", "golang", 0).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	_, err = client.LPush(ctx, "keywithnonstringvalue", "somevalue").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	_, err = client.LCS(ctx, &redis.LCSQuery{
		//		Key1: "keywithstringvalue",
		//		Key2: "keywithnonstringvalue",
		//	}).Result()
		//	Expect(err).To(HaveOccurred())
		//	Expect(err.Error()).To(Equal("ERR The specified keys must contain string values"))
		//})

		It("should LIndex", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lIndex := client.LIndex(ctx, "list", 0)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("Hello"))

			lIndex = client.LIndex(ctx, "list", -1)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("World"))

			lIndex = client.LIndex(ctx, "list", 3)
			Expect(lIndex.Err()).To(Equal(redis.Nil))
			Expect(lIndex.Val()).To(Equal(""))
		})

		It("should LInsert", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert(ctx, "list", "BEFORE", "World", "There")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange(ctx, "list", 0, -1)
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
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lLen := client.LLen(ctx, "list")
			Expect(lLen.Err()).NotTo(HaveOccurred())
			Expect(lLen.Val()).To(Equal(int64(2)))
		})

		// todo fix: https://github.com/OpenAtomFoundation/pika/issues/1791
		It("should LPop", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPop := client.LPop(ctx, "list")
			Expect(lPop.Err()).NotTo(HaveOccurred())
			Expect(lPop.Val()).To(Equal("one"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should LPopCount", func() {
			rPush := client.RPush(ctx, "list11", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list11", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list11", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list11", "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPopCount := client.LPopCount(ctx, "list11", 2)
			Expect(lPopCount.Err()).NotTo(HaveOccurred())
			Expect(lPopCount.Val()).To(Equal([]string{"one", "two"}))

			lRange := client.LRange(ctx, "list11", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three", "four"}))
		})

		//It("should LPos", func() {
		//	rPush := client.RPush(ctx, "list", "a")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "c")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//
		//	lPos := client.LPos(ctx, "list", "b", redis.LPosArgs{})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(1)))
		//
		//	lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: 2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(3)))
		//
		//	lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: -2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal(int64(1)))
		//
		//	lPos = client.LPos(ctx, "list", "b", redis.LPosArgs{Rank: 2, MaxLen: 1})
		//	Expect(lPos.Err()).To(Equal(redis.Nil))
		//
		//	lPos = client.LPos(ctx, "list", "z", redis.LPosArgs{})
		//	Expect(lPos.Err()).To(Equal(redis.Nil))
		//})

		//It("should LPosCount", func() {
		//	rPush := client.RPush(ctx, "list", "a")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "c")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	rPush = client.RPush(ctx, "list", "b")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//
		//	lPos := client.LPosCount(ctx, "list", "b", 2, redis.LPosArgs{})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{1, 3}))
		//
		//	lPos = client.LPosCount(ctx, "list", "b", 2, redis.LPosArgs{Rank: 2})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{3}))
		//
		//	lPos = client.LPosCount(ctx, "list", "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 1})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{}))
		//
		//	lPos = client.LPosCount(ctx, "list", "b", 1, redis.LPosArgs{Rank: 1, MaxLen: 0})
		//	Expect(lPos.Err()).NotTo(HaveOccurred())
		//	Expect(lPos.Val()).To(Equal([]int64{1}))
		//})

		It("should LPush", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush(ctx, "list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should LPushX", func() {
			lPush := client.LPush(ctx, "list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lPushX := client.LPushX(ctx, "list", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(2)))

			lPush = client.LPush(ctx, "list1", "three")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			Expect(lPush.Val()).To(Equal(int64(1)))

			lPushX = client.LPushX(ctx, "list1", "two", "one")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(3)))

			lPushX = client.LPushX(ctx, "list2", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, "list1", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRange", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange(ctx, "list", 0, 0)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one"}))

			lRange = client.LRange(ctx, "list", -3, 2)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list", -100, 100)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list", 5, 10)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRem", func() {
			rPush := client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "key")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRem := client.LRem(ctx, "list", -2, "hello")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"hello", "key"}))
		})

		It("should LRem binary", func() {
			rPush := client.RPush(ctx, "list", "\x00\xa2\x00")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "\x00\x9d")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert(ctx, "list", "BEFORE", "\x00\x9d", "\x00\x5f")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"\x00\xa2\x00", "\x00\x5f", "\x00\x9d"}))

			lRem := client.LRem(ctx, "list", -1, "\x00\x5f")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(1)))

			lRange = client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"\x00\xa2\x00", "\x00\x9d"}))
		})

		It("should LSet", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lSet := client.LSet(ctx, "list", 0, "four")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lSet = client.LSet(ctx, "list", -2, "five")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"four", "five", "three"}))
		})

		It("should LTrim", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lTrim := client.LTrim(ctx, "list", 1, -1)
			Expect(lTrim.Err()).NotTo(HaveOccurred())
			Expect(lTrim.Val()).To(Equal("OK"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		// todo fix: https://github.com/OpenAtomFoundation/pika/issues/1791
		It("should RPop", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPop := client.RPop(ctx, "list")
			Expect(rPop.Err()).NotTo(HaveOccurred())
			Expect(rPop.Val()).To(Equal("three"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPopCount", func() {
			rPush := client.RPush(ctx, "list", "one", "two", "three", "four")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(4)))

			rPopCount := client.RPopCount(ctx, "list", 2)
			Expect(rPopCount.Err()).NotTo(HaveOccurred())
			Expect(rPopCount.Val()).To(Equal([]string{"four", "three"}))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPopLPush", func() {
			rPush := client.RPush(ctx, "list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush(ctx, "list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPopLPush := client.RPopLPush(ctx, "list", "list2")
			Expect(rPopLPush.Err()).NotTo(HaveOccurred())
			Expect(rPopLPush.Val()).To(Equal("three"))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"three"}))
		})

		It("should RPush", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush(ctx, "list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should RPushX", func() {
			rPush := client.RPush(ctx, "list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX := client.RPushX(ctx, "list", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(2)))

			rPush = client.RPush(ctx, "list1", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX = client.RPushX(ctx, "list1", "two", "three")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(3)))

			rPushX = client.RPushX(ctx, "list2", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange(ctx, "list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange(ctx, "list1", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange(ctx, "list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		//It("should LMove", func() {
		//	rPush := client.RPush(ctx, "lmove1", "ichi")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(1)))
		//
		//	rPush = client.RPush(ctx, "lmove1", "ni")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(2)))
		//
		//	rPush = client.RPush(ctx, "lmove1", "san")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(3)))
		//
		//	lMove := client.LMove(ctx, "lmove1", "lmove2", "RIGHT", "LEFT")
		//	Expect(lMove.Err()).NotTo(HaveOccurred())
		//	Expect(lMove.Val()).To(Equal("san"))
		//
		//	lRange := client.LRange(ctx, "lmove2", 0, -1)
		//	Expect(lRange.Err()).NotTo(HaveOccurred())
		//	Expect(lRange.Val()).To(Equal([]string{"san"}))
		//})

		//It("should BLMove", func() {
		//	rPush := client.RPush(ctx, "blmove1", "ichi")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(1)))
		//
		//	rPush = client.RPush(ctx, "blmove1", "ni")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(2)))
		//
		//	rPush = client.RPush(ctx, "blmove1", "san")
		//	Expect(rPush.Err()).NotTo(HaveOccurred())
		//	Expect(rPush.Val()).To(Equal(int64(3)))
		//
		//	blMove := client.BLMove(ctx, "blmove1", "blmove2", "RIGHT", "LEFT", time.Second)
		//	Expect(blMove.Err()).NotTo(HaveOccurred())
		//	Expect(blMove.Val()).To(Equal("san"))
		//
		//	lRange := client.LRange(ctx, "blmove2", 0, -1)
		//	Expect(lRange.Err()).NotTo(HaveOccurred())
		//	Expect(lRange.Val()).To(Equal([]string{"san"}))
		//})
	})
})
