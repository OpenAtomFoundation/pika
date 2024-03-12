package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sort"
	"strconv"
	"time"
)

var _ = Describe("List Commands Codis", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(CODISADDR))
	})

	AfterEach(func() {
		//Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("lists", func() {
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
var _ = Describe("Hash Commands Codis", func() {
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
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hDel := client.HDel(ctx, hashKey, "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(1)))

			hDel = client.HDel(ctx, hashKey, "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(0)))
		})

		It("should HExists", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hExists := client.HExists(ctx, hashKey, "key")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(true))

			hExists = client.HExists(ctx, hashKey, "key1")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(false))
		})

		It("should HGet", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, hashKey, "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			hGet = client.HGet(ctx, hashKey, "key1")
			Expect(hGet.Err()).To(Equal(redis.Nil))
			Expect(hGet.Val()).To(Equal(""))
		})

		It("should HGetAll", func() {
			hashKey := uuid.New().String()

			err := client.HSet(ctx, hashKey, "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, hashKey, "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			m, err := client.HGetAll(ctx, hashKey).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("should scan", func() {
			hashKey := uuid.New().String()
			now := time.Now()

			err := client.HMSet(ctx, hashKey, "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.HGetAll(ctx, hashKey)
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
			////err = client.HSet(ctx, hashKey, &data2{
			////	Key1: "hello2",
			////	Key2: 200,
			////	Time: now,
			////}).Err()
			////Expect(err).NotTo(HaveOccurred())
			//
			//var d2 data2
			//err = client.HMGet(ctx, hashKey, "key1", "key2", "time").Scan(&d2)
			//Expect(err).NotTo(HaveOccurred())
			//Expect(d2.Key1).To(Equal("hello2"))
			//Expect(d2.Key2).To(Equal(200))
			//Expect(d2.Time.Unix()).To(Equal(now.Unix()))
		})

		It("should HIncrBy", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key", "5")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hIncrBy := client.HIncrBy(ctx, hashKey, "key", 1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(6)))

			hIncrBy = client.HIncrBy(ctx, hashKey, "key", -1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(5)))

			hIncrBy = client.HIncrBy(ctx, hashKey, "key", -10)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(-5)))
		})

		It("should HIncrByFloat", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "field", "10.50")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hIncrByFloat := client.HIncrByFloat(ctx, hashKey, "field", 0.1)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(10.6))

			hSet = client.HSet(ctx, hashKey, "field", "5.0e3")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(0)))

			hIncrByFloat = client.HIncrByFloat(ctx, hashKey, "field", 2.0e2)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should HKeys", func() {
			hashKey := uuid.New().String()

			hkeys := client.HKeys(ctx, hashKey)
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{}))

			hset := client.HSet(ctx, hashKey, "key1", "hello1")
			Expect(hset.Err()).NotTo(HaveOccurred())
			hset = client.HSet(ctx, hashKey, "key2", "hello2")
			Expect(hset.Err()).NotTo(HaveOccurred())

			hkeys = client.HKeys(ctx, hashKey)
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{"key1", "key2"}))
		})

		It("should HLen", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			hSet = client.HSet(ctx, hashKey, "key2", "hello2")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hLen := client.HLen(ctx, hashKey)
			Expect(hLen.Err()).NotTo(HaveOccurred())
			Expect(hLen.Val()).To(Equal(int64(2)))
		})

		It("should HMGet", func() {
			hashKey := uuid.New().String()

			err := client.HSet(ctx, hashKey, "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.HMGet(ctx, hashKey, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"hello1"}))
		})

		It("should HSet", func() {
			hashKey := uuid.New().String()

			_, err := client.Del(ctx, hashKey).Result()
			Expect(err).NotTo(HaveOccurred())

			ok, err := client.HSet(ctx, hashKey, map[string]interface{}{
				"key1": "hello1",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			ok, err = client.HSet(ctx, hashKey, map[string]interface{}{
				"key2": "hello2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			v, err := client.HGet(ctx, hashKey, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello1"))

			v, err = client.HGet(ctx, hashKey, "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello2"))

			keys, err := client.HKeys(ctx, hashKey).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf([]string{"key1", "key2"}))
		})

		It("should HSet", func() {
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hGet := client.HGet(ctx, hashKey, "key")
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
			//hSet = client.HSet(ctx, hashKey, &set{
			//	Set1: "val1",
			//	Set2: 1024,
			//	Set3: 2 * time.Millisecond,
			//	Set4: nil,
			//	Set5: map[string]interface{}{"k1": 1},
			//})
			//Expect(hSet.Err()).NotTo(HaveOccurred())
			//Expect(hSet.Val()).To(Equal(int64(4)))

			//hMGet := client.HMGet(ctx, hashKey, "set1", "set2", "set3", "set4", "set5", "set6")
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
			hashKey := uuid.New().String()

			res := client.Del(ctx, hashKey)
			Expect(res.Err()).NotTo(HaveOccurred())

			hSetNX := client.HSetNX(ctx, hashKey, "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(true))

			hSetNX = client.HSetNX(ctx, hashKey, "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(false))

			hGet := client.HGet(ctx, hashKey, "key")
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
			hashKey := uuid.New().String()

			hSet := client.HSet(ctx, hashKey, "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, hashKey, "key1")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			length := client.Do(ctx, "hstrlen", hashKey, "key1")

			Expect(length.Val()).To(Equal(int64(len("hello1"))))
		})

	})
})
var _ = FDescribe("String Commands Codis", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(CODISADDR))
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	FDescribe("strings", func() {
		It("should Append", func() {
			key := uuid.New().String()

			n, err := client.Exists(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			appendRes := client.Append(ctx, key, "Hello")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(5)))

			appendRes = client.Append(ctx, key, " World")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, key)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello World"))
		})

		It("should BitCount", func() {
			key := uuid.New().String()

			set := client.Set(ctx, key, "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount(ctx, key, nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount(ctx, key, &redis.BitCount{
				Start: 0,
				End:   0,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount(ctx, key, &redis.BitCount{
				Start: 1,
				End:   1,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should BitPos", func() {
			key := uuid.New().String()

			err := client.Set(ctx, key, "\xff\xf0\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPos(ctx, key, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(12)))

			pos, err = client.BitPos(ctx, key, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(0)))

			pos, err = client.BitPos(ctx, key, 0, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, key, 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, key, 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, key, 1, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, key, 0, 2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			//pos, err = client.BitPos(ctx, key, 0, 0, -3).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(pos).To(Equal(int64(-1)))

			//pos, err = client.BitPos(ctx, key, 0, 0, 0).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(pos).To(Equal(int64(-1)))
		})

		It("should BitPosSpan", func() {
			key := uuid.New().String()

			err := client.Set(ctx, key, "\x00\xff\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		})

		// fix: https://github.com/OpenAtomFoundation/pika/issues/2061
		It("should Decr", func() {
			key := uuid.New().String()

			basicSet := client.Set(ctx, key, "10", 0)
			Expect(basicSet.Err()).NotTo(HaveOccurred())
			Expect(basicSet.Val()).To(Equal("OK"))
			basicDecr := client.Decr(ctx, key)
			Expect(basicDecr.Err()).NotTo(HaveOccurred())
			Expect(basicDecr.Val()).To(Equal(int64(9)))
			basicDecr = client.Decr(ctx, key)
			Expect(basicDecr.Err()).NotTo(HaveOccurred())
			Expect(basicDecr.Val()).To(Equal(int64(8)))

			for i := 0; i < 5; i++ {
				set := client.Set(ctx, key, "234293482390480948029348230948", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
				Expect(set.Val()).To(Equal("OK"))
				decr := client.Decr(ctx, key)
				Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))

				set = client.Set(ctx, key, "-9223372036854775809", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
				Expect(set.Val()).To(Equal("OK"))
				decr = client.Decr(ctx, key)
				Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))

				inter := randomInt(500)
				set = client.Set(ctx, key, inter, 0)
				for j := 0; j < 200; j++ {
					res := client.Decr(ctx, key)
					Expect(res.Err()).NotTo(HaveOccurred())
				}
			}
		})

		It("should DecrBy", func() {
			key := uuid.New().String()

			set := client.Set(ctx, key, "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decrBy := client.DecrBy(ctx, key, 5)
			Expect(decrBy.Err()).NotTo(HaveOccurred())
			Expect(decrBy.Val()).To(Equal(int64(5)))
		})

		It("should Get", func() {
			key := uuid.New().String()

			get := client.Get(ctx, "_")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			set := client.Set(ctx, key, "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get = client.Get(ctx, key)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetBit", func() {
			setBit := client.SetBit(ctx, "key_3s", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			Expect(client.Expire(ctx, "key_3s", 3*time.Second).Val()).To(Equal(true))
			Expect(client.TTL(ctx, "key_3s").Val()).NotTo(Equal(int64(-2)))

			setBit = client.SetBit(ctx, "key_3s", 69, 1)
			Expect(client.TTL(ctx, "key_3s").Val()).NotTo(Equal(int64(-2)))
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			time.Sleep(4 * time.Second)
			Expect(client.TTL(ctx, "key_3s").Val()).To(Equal(time.Duration(-2)))
		})

		It("should GetBit", func() {
			key := uuid.New().String()

			setBit := client.SetBit(ctx, key, 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit(ctx, key, 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit(ctx, key, 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit(ctx, key, 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
		})

		It("should GetRange", func() {
			key := uuid.New().String()

			set := client.Set(ctx, key, "This is a string", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getRange := client.GetRange(ctx, key, 0, 3)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This"))

			getRange = client.GetRange(ctx, key, -3, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("ing"))

			getRange = client.GetRange(ctx, key, 0, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This is a string"))

			getRange = client.GetRange(ctx, key, 10, 100)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("string"))
		})

		It("should GetSet", func() {
			key := uuid.New().String()

			incr := client.Incr(ctx, key)
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(1)))

			getSet := client.GetSet(ctx, key, "0")
			Expect(getSet.Err()).NotTo(HaveOccurred())
			Expect(getSet.Val()).To(Equal("1"))

			get := client.Get(ctx, key)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})
		//
		//It("should GetEX", func() {
		//	set := client.Set(ctx, "key", "value", 100*time.Second)
		//	Expect(set.Err()).NotTo(HaveOccurred())
		//	Expect(set.Val()).To(Equal("OK"))
		//
		//	ttl := client.TTL(ctx, "key")
		//	Expect(ttl.Err()).NotTo(HaveOccurred())
		//	Expect(ttl.Val()).To(BeNumerically("~", 100*time.Second, 3*time.Second))
		//
		//	getEX := client.GetEx(ctx, "key", 200*time.Second)
		//	Expect(getEX.Err()).NotTo(HaveOccurred())
		//	Expect(getEX.Val()).To(Equal("value"))
		//
		//	ttl = client.TTL(ctx, "key")
		//	Expect(ttl.Err()).NotTo(HaveOccurred())
		//	Expect(ttl.Val()).To(BeNumerically("~", 200*time.Second, 3*time.Second))
		//})

		//It("should GetDel", func() {
		//	set := client.Set(ctx, "key", "value", 0)
		//	Expect(set.Err()).NotTo(HaveOccurred())
		//	Expect(set.Val()).To(Equal("OK"))
		//
		//	getDel := client.GetDel(ctx, "key")
		//	Expect(getDel.Err()).NotTo(HaveOccurred())
		//	Expect(getDel.Val()).To(Equal("value"))
		//
		//	get := client.Get(ctx, "key")
		//	Expect(get.Err()).To(Equal(redis.Nil))
		//})

		It("should Incr", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("11"))
		})

		It("should IncrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrBy := client.IncrBy(ctx, "key", 5)
			Expect(incrBy.Err()).NotTo(HaveOccurred())
			Expect(incrBy.Val()).To(Equal(int64(15)))
		})

		It("should IncrByFloat", func() {
			set := client.Set(ctx, "key", "10.50", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat := client.IncrByFloat(ctx, "key", 0.1)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(10.6))

			set = client.Set(ctx, "key", "5.0e3", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat = client.IncrByFloat(ctx, "key", 2.0e2)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should IncrByFloatOverflow", func() {
			incrByFloat := client.IncrByFloat(ctx, "key", 996945661)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(996945661)))
		})

		It("should MSetMGet", func() {
			key := uuid.New().String()
			key2 := uuid.New().String()

			mSet := client.MSet(ctx, key, "hello1", key2, "hello2")
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet := client.MGet(ctx, key, key2, "_")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{"hello1", "hello2", nil}))

			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSet = client.MSet(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet = client.MGet(ctx, "set1", "set2", "set3", "set4")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{
				"val1",
				"1024",
				strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
				"",
			}))
		})

		It("should scan Mget", func() {
			now := time.Now()

			err := client.MSet(ctx, "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.MGet(ctx, "key1", "key2", "_", "time")
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
		})

		It("should SetWithArgs with keepttl", func() {
			key := uuid.New().String()

			// Set with ttl
			argsWithTTL := redis.SetArgs{
				TTL: 5 * time.Second,
			}
			set := client.SetArgs(ctx, key, "hello", argsWithTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Result()).To(Equal("OK"))

			// Set with keepttl
			//argsWithKeepTTL := redis.SetArgs{
			//	KeepTTL: true,
			//}
			//set = client.SetArgs(ctx, "key", "hello567", argsWithKeepTTL)
			//Expect(set.Err()).NotTo(HaveOccurred())
			//Expect(set.Result()).To(Equal("OK"))
			//
			//ttl := client.TTL(ctx, "key")
			//Expect(ttl.Err()).NotTo(HaveOccurred())
			//// set keepttl will Retain the ttl associated with the key
			//Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetWithArgs with NX mode and key exists", func() {
			key := uuid.New().String()

			err := client.Set(ctx, key, "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, key, "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with NX mode and key does not exist", func() {
			key := uuid.New().String()
			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, key, "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should SetWithArgs with expiration, NX mode, and key exists", func() {
			key := uuid.New().String()
			e := client.Set(ctx, key, "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, key, "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key does not exist", func() {
			key := uuid.New().String()
			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, key, "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key exists", func() {
			key := uuid.New().String()
			e := client.Set(ctx, key, "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, key, "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should Set with keepttl", func() {
			key := uuid.New().String()
			// set with ttl
			set := client.Set(ctx, key, "hello", 5*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			// set with keepttl
			// mset key1 hello1 key2 123 time 2023-05-19T15:42:06.880088+08:00
			//set = client.Set(ctx, key, "hello1", redis.KeepTTL)
			//Expect(set.Err()).NotTo(HaveOccurred())
			//Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, key)
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetGet", func() {
			key := uuid.New().String()

			set := client.Set(ctx, key, "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get := client.Get(ctx, key)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetEX", func() {
			key := uuid.New().String()

			err := client.SetEx(ctx, key, "hello", 1*time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "foo").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetNX", func() {
			key := uuid.New().String()

			_, err := client.Del(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())

			setNX := client.SetNX(ctx, key, "hello", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(true))

			setNX = client.SetNX(ctx, key, "hello2", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(false))

			get := client.Get(ctx, key)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX with expiration", func() {
			key := uuid.New().String()

			_, err := client.Del(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())

			isSet, err := client.SetNX(ctx, key, "hello", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetNX(ctx, key, "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			val, err := client.Get(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetXX", func() {
			key := uuid.New().String()

			isSet, err := client.SetXX(ctx, key, "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, key, "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, key, "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with expiration", func() {
			key := uuid.New().String()
			isSet, err := client.SetXX(ctx, key, "hello2", time.Second*1000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, key, "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, key, "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with keepttl", func() {
			key := uuid.New().String()

			isSet, err := client.SetXX(ctx, key, "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, key, "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, key, "hello2", 5*time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			//isSet, err = client.SetXX(ctx, key, "hello3", redis.KeepTTL).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))

			// set keepttl will Retain the ttl associated with the key
			ttl, err := client.TTL(ctx, key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ttl).NotTo(Equal(-1))
		})

		It("should SetRange", func() {
			key := uuid.New().String()
			key2 := uuid.New().String()
			setRes := client.Set(ctx, key, "", 0)
			Expect(setRes.Err()).NotTo(HaveOccurred())
			Expect(setRes.Val()).To(Equal("OK"))

			getRes := client.Get(ctx, key)
			Expect(getRes.Err()).NotTo(HaveOccurred())
			Expect(getRes.Val()).To(Equal(""))

			setRangeRes := client.SetRange(ctx, key, 0, "Pika")
			Expect(setRangeRes.Err()).NotTo(HaveOccurred())
			Expect(setRangeRes.Val()).To(Equal(int64(4)))

			getRes = client.Get(ctx, key)
			Expect(getRes.Err()).NotTo(HaveOccurred())
			Expect(getRes.Val()).To(Equal("Pika"))

			set := client.Set(ctx, key2, "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			Expect(client.Expire(ctx, key2, 3*time.Second).Val()).To(Equal(true))
			Expect(client.TTL(ctx, "key_3s").Val()).NotTo(Equal(int64(-2)))

			range_ := client.SetRange(ctx, key2, 6, "Redis")
			Expect(range_.Err()).NotTo(HaveOccurred())
			Expect(range_.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, key2)
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello Redis"))
			Expect(client.TTL(ctx, key2).Val()).NotTo(Equal(int64(-2)))

			time.Sleep(4 * time.Second)
			Expect(client.TTL(ctx, "key_3s").Val()).To(Equal(time.Duration(-2)))
		})

		It("should StrLen", func() {
			key := uuid.New().String()

			set := client.Set(ctx, key, "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			strLen := client.StrLen(ctx, key)
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(5)))

			strLen = client.StrLen(ctx, "_")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(0)))
		})

	})
})
