package pika_integration

import (
	"context"
	"strconv"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("String Commands", func() {
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

	Describe("strings", func() {
		It("should Append", func() {
			n, err := client.Exists(ctx, "key__11").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			appendRes := client.Append(ctx, "key", "Hello")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(5)))

			appendRes = client.Append(ctx, "key", " World")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello World"))
		})

		It("should BitCount", func() {
			set := client.Set(ctx, "key", "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount(ctx, "key", nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 0,
				End:   0,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 1,
				End:   1,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should BitOpAnd", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpAnd := client.BitOpAnd(ctx, "dest", "key1", "key2")
			Expect(bitOpAnd.Err()).NotTo(HaveOccurred())
			Expect(bitOpAnd.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should BitOpOr", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpOr := client.BitOpOr(ctx, "dest", "key1", "key2")
			Expect(bitOpOr.Err()).NotTo(HaveOccurred())
			Expect(bitOpOr.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("1"))
		})

		It("should BitOpXor", func() {
			set := client.Set(ctx, "key1", "\xff", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "\x0f", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpXor := client.BitOpXor(ctx, "dest", "key1", "key2")
			Expect(bitOpXor.Err()).NotTo(HaveOccurred())
			Expect(bitOpXor.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xf0"))
		})

		It("should BitOpNot", func() {
			set := client.Set(ctx, "key1", "\x00", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpNot := client.BitOpNot(ctx, "dest", "key1")
			Expect(bitOpNot.Err()).NotTo(HaveOccurred())
			Expect(bitOpNot.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xff"))
		})

		It("should BitPos", func() {
			err := client.Set(ctx, "mykey", "\xff\xf0\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPos(ctx, "mykey", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(12)))

			pos, err = client.BitPos(ctx, "mykey", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(0)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			//pos, err = client.BitPos(ctx, "mykey", 0, 0, -3).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(pos).To(Equal(int64(-1)))

			//pos, err = client.BitPos(ctx, "mykey", 0, 0, 0).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(pos).To(Equal(int64(-1)))
		})

		It("should BitPosSpan", func() {
			err := client.Set(ctx, "mykey", "\x00\xff\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		})

		// fix: https://github.com/OpenAtomFoundation/pika/issues/2061
		It("should Decr", func() {
			basicSet := client.Set(ctx, "mykey", "10", 0)
			Expect(basicSet.Err()).NotTo(HaveOccurred())
			Expect(basicSet.Val()).To(Equal("OK"))
			basicDecr := client.Decr(ctx, "mykey")
			Expect(basicDecr.Err()).NotTo(HaveOccurred())
			Expect(basicDecr.Val()).To(Equal(int64(9)))
			basicDecr = client.Decr(ctx, "mykey")
			Expect(basicDecr.Err()).NotTo(HaveOccurred())
			Expect(basicDecr.Val()).To(Equal(int64(8)))

			for i := 0; i < 5; i++ {
				set := client.Set(ctx, "key", "234293482390480948029348230948", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
				Expect(set.Val()).To(Equal("OK"))
				decr := client.Decr(ctx, "key")
				Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))

				set = client.Set(ctx, "key", "-9223372036854775809", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
				Expect(set.Val()).To(Equal("OK"))
				decr = client.Decr(ctx, "key")
				Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))

				inter := randomInt(500)
				set = client.Set(ctx, "key", inter, 0)
				for j := 0; j < 200; j++ {
					res := client.Decr(ctx, "key")
					Expect(res.Err()).NotTo(HaveOccurred())
				}
			}
		})

		It("should DecrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decrBy := client.DecrBy(ctx, "key", 5)
			Expect(decrBy.Err()).NotTo(HaveOccurred())
			Expect(decrBy.Val()).To(Equal(int64(5)))
		})

		It("should Get", func() {
			get := client.Get(ctx, "_")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get = client.Get(ctx, "key")
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
			setBit := client.SetBit(ctx, "key", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit(ctx, "key", 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit(ctx, "key", 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit(ctx, "key", 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
		})

		It("should GetRange", func() {
			set := client.Set(ctx, "key", "This is a string", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getRange := client.GetRange(ctx, "key", 0, 3)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This"))

			getRange = client.GetRange(ctx, "key", -3, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("ing"))

			getRange = client.GetRange(ctx, "key", 0, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This is a string"))

			getRange = client.GetRange(ctx, "key", 10, 100)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("string"))
		})

		It("should GetSet", func() {
			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(1)))

			getSet := client.GetSet(ctx, "key", "0")
			Expect(getSet.Err()).NotTo(HaveOccurred())
			Expect(getSet.Val()).To(Equal("1"))

			get := client.Get(ctx, "key")
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
			mSet := client.MSet(ctx, "key1", "hello1", "key2", "hello2")
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet := client.MGet(ctx, "key1", "key2", "_")
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

		It("should MSetNX", func() {
			mSetNX := client.MSetNX(ctx, "MSetNXkey1", "hello1", "MSetNXkey2", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))

			mSetNX = client.MSetNX(ctx, "MSetNXkey1", "hello1", "MSetNXkey2", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(false))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSetNX = client.MSetNX(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))
		})

		//It("should SetWithArgs with TTL", func() {
		//	args := redis.SetArgs{
		//		TTL: 500 * time.Millisecond,
		//	}
		//	err := client.SetArgs(ctx, "key", "hello", args).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	val, err := client.Get(ctx, "key").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//
		//	Eventually(func() error {
		//		return client.Get(ctx, "key").Err()
		//	}, "2s", "100ms").Should(Equal(redis.Nil))
		//})

		// todo 语法不对，单独支持
		//It("should SetWithArgs with expiration date", func() {
		//	expireAt := time.Now().AddDate(1, 1, 1)
		//	args := redis.SetArgs{
		//		ExpireAt: expireAt,
		//	}
		//	err := client.SetArgs(ctx, "key", "hello", args).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	val, err := client.Get(ctx, "key").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//
		//	// check the key has an expiration date
		//	// (so a TTL value different of -1)
		//	ttl := client.TTL(ctx, "key")
		//	Expect(ttl.Err()).NotTo(HaveOccurred())
		//	Expect(ttl.Val()).ToNot(Equal(-1))
		//})

		//It("should SetWithArgs with negative expiration date", func() {
		//	args := redis.SetArgs{
		//		ExpireAt: time.Now().AddDate(-3, 1, 1),
		//	}
		//	// redis accepts a timestamp less than the current date
		//	// but returns nil when trying to get the key
		//	err := client.SetArgs(ctx, "key", "hello", args).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	val, err := client.Get(ctx, "key").Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		It("should SetWithArgs with keepttl", func() {
			// Set with ttl
			argsWithTTL := redis.SetArgs{
				TTL: 5 * time.Second,
			}
			set := client.SetArgs(ctx, "key", "hello", argsWithTTL)
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
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with NX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		// todo 待支持
		//It("should SetWithArgs with NX mode and GET option", func() {
		//	args := redis.SetArgs{
		//		Mode: "nx",
		//		Get:  true,
		//	}
		//	val, err := client.SetArgs(ctx, "key", "hello", args).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		//It("should SetWithArgs with expiration, NX mode, and key does not exist", func() {
		//	args := redis.SetArgs{
		//		TTL:  500 * time.Millisecond,
		//		Mode: "nx",
		//	}
		//	val, err := client.SetArgs(ctx, "key", "hello", args).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("OK"))
		//
		//	Eventually(func() error {
		//		return client.Get(ctx, "key").Err()
		//	}, "1s", "100ms").Should(Equal(redis.Nil))
		//})

		It("should SetWithArgs with expiration, NX mode, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		//It("should SetWithArgs with expiration, NX mode, and GET option", func() {
		//	args := redis.SetArgs{
		//		TTL:  500 * time.Millisecond,
		//		Mode: "nx",
		//		Get:  true,
		//	}
		//	val, err := client.SetArgs(ctx, "key", "hello", args).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		It("should SetWithArgs with XX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		//It("should SetWithArgs with XX mode and GET option, and key exists", func() {
		//	e := client.Set(ctx, "key", "hello", 0).Err()
		//	Expect(e).NotTo(HaveOccurred())
		//
		//	args := redis.SetArgs{
		//		Mode: "xx",
		//		Get:  true,
		//	}
		//	val, err := client.SetArgs(ctx, "key", "world", args).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//})

		//It("should SetWithArgs with XX mode and GET option, and key does not exist", func() {
		//	args := redis.SetArgs{
		//		Mode: "xx",
		//		Get:  true,
		//	}
		//
		//	val, err := client.SetArgs(ctx, "key", "world", args).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		//It("should SetWithArgs with expiration, XX mode, GET option, and key does not exist", func() {
		//	args := redis.SetArgs{
		//		TTL:  500 * time.Millisecond,
		//		Mode: "xx",
		//		Get:  true,
		//	}
		//
		//	val, err := client.SetArgs(ctx, "key", "world", args).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		//It("should SetWithArgs with expiration, XX mode, GET option, and key exists", func() {
		//	e := client.Set(ctx, "key", "hello", 0)
		//	Expect(e.Err()).NotTo(HaveOccurred())
		//
		//	args := redis.SetArgs{
		//		TTL:  500 * time.Millisecond,
		//		Mode: "xx",
		//		Get:  true,
		//	}
		//
		//	val, err := client.SetArgs(ctx, "key", "world", args).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//
		//	Eventually(func() error {
		//		return client.Get(ctx, "key").Err()
		//	}, "1s", "100ms").Should(Equal(redis.Nil))
		//})

		//It("should SetWithArgs with Get and key does not exist yet", func() {
		//	args := redis.SetArgs{
		//		Get: true,
		//	}
		//
		//	val, err := client.SetArgs(ctx, "key", "hello", args).Result()
		//	Expect(err).To(Equal(redis.Nil))
		//	Expect(val).To(Equal(""))
		//})

		//It("should SetWithArgs with Get and key exists", func() {
		//	e := client.Set(ctx, "key", "hello", 0)
		//	Expect(e.Err()).NotTo(HaveOccurred())
		//
		//	args := redis.SetArgs{
		//		Get: true,
		//	}
		//
		//	val, err := client.SetArgs(ctx, "key", "world", args).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//})

		//It("should Pipelined SetArgs with Get and key exists", func() {
		//	e := client.Set(ctx, "key", "hello", 0)
		//	Expect(e.Err()).NotTo(HaveOccurred())
		//
		//	args := redis.SetArgs{
		//		Get: true,
		//	}
		//
		//	pipe := client.Pipeline()
		//	setArgs := pipe.SetArgs(ctx, "key", "world", args)
		//	_, err := pipe.Exec(ctx)
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	Expect(setArgs.Err()).NotTo(HaveOccurred())
		//	Expect(setArgs.Val()).To(Equal("hello"))
		//})

		//It("should Set with expiration", func() {
		//	err := client.Set(ctx, "key", "hello", 100*time.Millisecond).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	val, err := client.Get(ctx, "key").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal("hello"))
		//
		//	Eventually(func() error {
		//		return client.Get(ctx, "key").Err()
		//	}, "1s", "100ms").Should(Equal(redis.Nil))
		//})

		It("should Set with keepttl", func() {
			// set with ttl
			set := client.Set(ctx, "key", "hello", 5*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			// set with keepttl
			// mset key1 hello1 key2 123 time 2023-05-19T15:42:06.880088+08:00
			//set = client.Set(ctx, "key", "hello1", redis.KeepTTL)
			//Expect(set.Err()).NotTo(HaveOccurred())
			//Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetGet", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetEX", func() {
			err := client.SetEx(ctx, "key", "hello", 1*time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "foo").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetNX", func() {
			_, err := client.Del(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())

			setNX := client.SetNX(ctx, "key", "hello", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(true))

			setNX = client.SetNX(ctx, "key", "hello2", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(false))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX with expiration", func() {
			_, err := client.Del(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())

			isSet, err := client.SetNX(ctx, "key", "hello", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetNX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		//It("should SetNX with keepttl", func() {
		//	isSet, err := client.SetNX(ctx, "key", "hello1", redis.KeepTTL).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(isSet).To(Equal(true))
		//
		//	ttl := client.TTL(ctx, "key")
		//	Expect(ttl.Err()).NotTo(HaveOccurred())
		//	Expect(ttl.Val().Nanoseconds()).To(Equal(int64(-1)))
		//})

		It("should SetXX", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with expiration", func() {
			isSet, err := client.SetXX(ctx, "SetXXkey11111", "hello2", time.Second*1000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "SetXXkey11111", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "SetXXkey11111", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "SetXXkey11111").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with keepttl", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 5*time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			//isSet, err = client.SetXX(ctx, "key", "hello3", redis.KeepTTL).Result()
			//Expect(err).NotTo(HaveOccurred())
			//Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))

			// set keepttl will Retain the ttl associated with the key
			ttl, err := client.TTL(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ttl).NotTo(Equal(-1))
		})

		It("should SetRange", func() {
			setRes := client.Set(ctx, "nil_key", "", 0)
			Expect(setRes.Err()).NotTo(HaveOccurred())
			Expect(setRes.Val()).To(Equal("OK"))

			getRes := client.Get(ctx, "nil_key")
			Expect(getRes.Err()).NotTo(HaveOccurred())
			Expect(getRes.Val()).To(Equal(""))

			setRangeRes := client.SetRange(ctx, "nil_key", 0, "Pika")
			Expect(setRangeRes.Err()).NotTo(HaveOccurred())
			Expect(setRangeRes.Val()).To(Equal(int64(4)))

			getRes = client.Get(ctx, "nil_key")
			Expect(getRes.Err()).NotTo(HaveOccurred())
			Expect(getRes.Val()).To(Equal("Pika"))

			set := client.Set(ctx, "key_3s", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			Expect(client.Expire(ctx, "key_3s", 3*time.Second).Val()).To(Equal(true))
			Expect(client.TTL(ctx, "key_3s").Val()).NotTo(Equal(int64(-2)))

			range_ := client.SetRange(ctx, "key_3s", 6, "Redis")
			Expect(range_.Err()).NotTo(HaveOccurred())
			Expect(range_.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key_3s")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello Redis"))
			Expect(client.TTL(ctx, "key_3s").Val()).NotTo(Equal(int64(-2)))

			time.Sleep(4 * time.Second)
			Expect(client.TTL(ctx, "key_3s").Val()).To(Equal(time.Duration(-2)))
		})

		It("should StrLen", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			strLen := client.StrLen(ctx, "key")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(5)))

			strLen = client.StrLen(ctx, "_")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(0)))
		})

		//It("should Copy", func() {
		//	set := client.Set(ctx, "key", "hello", 0)
		//	Expect(set.Err()).NotTo(HaveOccurred())
		//	Expect(set.Val()).To(Equal("OK"))
		//
		//	copy := client.Copy(ctx, "key", "newKey", redisOptions().DB, false)
		//	Expect(copy.Err()).NotTo(HaveOccurred())
		//	Expect(copy.Val()).To(Equal(int64(1)))
		//
		//	// Value is available by both keys now
		//	getOld := client.Get(ctx, "key")
		//	Expect(getOld.Err()).NotTo(HaveOccurred())
		//	Expect(getOld.Val()).To(Equal("hello"))
		//	getNew := client.Get(ctx, "newKey")
		//	Expect(getNew.Err()).NotTo(HaveOccurred())
		//	Expect(getNew.Val()).To(Equal("hello"))
		//
		//	// Overwriting an existing key should not succeed
		//	overwrite := client.Copy(ctx, "newKey", "key", redisOptions().DB, false)
		//	Expect(overwrite.Val()).To(Equal(int64(0)))
		//
		//	// Overwrite is allowed when replace=rue
		//	replace := client.Copy(ctx, "newKey", "key", redisOptions().DB, true)
		//	Expect(replace.Val()).To(Equal(int64(1)))
		//})

		//It("should acl dryrun", func() {
		//	dryRun := client.ACLDryRun(ctx, "default", "get", "randomKey")
		//	Expect(dryRun.Err()).NotTo(HaveOccurred())
		//	Expect(dryRun.Val()).To(Equal("OK"))
		//})

		//It("should fail module loadex", func() {
		//	dryRun := client.ModuleLoadex(ctx, &redis.ModuleLoadexConfig{
		//		Path: "/path/to/non-existent-library.so",
		//		Conf: map[string]interface{}{
		//			"param1": "value1",
		//		},
		//		Args: []interface{}{
		//			"arg1",
		//		},
		//	})
		//	Expect(dryRun.Err()).To(HaveOccurred())
		//	Expect(dryRun.Err().Error()).To(Equal("ERR Error loading the extension. Please check the server logs."))
		//})

	})
})
