package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Cache test", func() {
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

	It("should Exists", func() {
		set := client.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		lPush := client.LPush(ctx, "key2", "b")
		Expect(lPush.Err()).NotTo(HaveOccurred())

		sAdd := client.SAdd(ctx, "key3", "c")
		Expect(sAdd.Err()).NotTo(HaveOccurred())
		Expect(sAdd.Val()).To(Equal(int64(1)))

		n, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		get := client.Get(ctx, "key1")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("a"))

		n1, err1 := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(3)))
	})

	It("should TTL", func() {
		set := client.Set(ctx, "key1", "bcd", 10*time.Minute)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))

		get := client.Get(ctx, "key1")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("bcd"))
		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))

		_, err := client.Del(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())

		set1 := client.Set(ctx, "key1", "bcd", 10*time.Minute)
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))
		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))

		mGet := client.MGet(ctx, "key1")
		Expect(mGet.Err()).NotTo(HaveOccurred())
		Expect(mGet.Val()).To(Equal([]interface{}{"bcd"}))

		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))
	})

	It("should TTL effective", func() {
		set := client.Set(ctx, "key1", "a", 10*time.Minute)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set1 := client.Set(ctx, "key2", "b", 10*time.Minute)
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key3", "c", 10*time.Minute)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key4", "d", 10*time.Minute)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		get := client.Get(ctx, "key1")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("a"))
		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))

		mGet := client.MGet(ctx, "key2")
		Expect(mGet.Err()).NotTo(HaveOccurred())
		Expect(mGet.Val()).To(Equal([]interface{}{"b"}))

		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))
		Expect(client.TTL(ctx, "key2").Val()).NotTo(Equal(int64(-2)))
		Expect(client.TTL(ctx, "key3").Val()).NotTo(Equal(int64(-2)))
		Expect(client.TTL(ctx, "key3").Val()).NotTo(Equal(int64(-2)))

		get1 := client.Get(ctx, "key1")
		Expect(get1.Err()).NotTo(HaveOccurred())
		Expect(get1.Val()).To(Equal("a"))

		get2 := client.Get(ctx, "key2")
		Expect(get2.Err()).NotTo(HaveOccurred())
		Expect(get2.Val()).To(Equal("b"))

		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))
		Expect(client.TTL(ctx, "key2").Val()).NotTo(Equal(int64(-2)))
	})

	It("should mget", func() {
		set := client.Set(ctx, "key1", "a", 10*time.Minute)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set1 := client.Set(ctx, "key2", "b", 10*time.Minute)
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key3", "c", 10*time.Minute)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key4", "d", 10*time.Minute)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		get := client.Get(ctx, "key1")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("a"))
		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(int64(-2)))

		mGet := client.MGet(ctx, "key2")
		Expect(mGet.Err()).NotTo(HaveOccurred())
		Expect(mGet.Val()).To(Equal([]interface{}{"b"}))

		mGet2 := client.MGet(ctx, "key1", "key2", "key3", "key4")
		Expect(mGet2.Err()).NotTo(HaveOccurred())
		Expect(mGet2.Val()).To(Equal([]interface{}{"a", "b", "c", "d"}))
	})

	It("should mget with ttl", func() {
		set := client.Set(ctx, "key1", "a", 3000*time.Millisecond)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set1 := client.Set(ctx, "key2", "b", 3000*time.Millisecond)
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key3", "c", 3000*time.Millisecond)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key4", "d", 3000*time.Millisecond)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		mget := client.MGet(ctx, "key1")
		Expect(mget.Err()).NotTo(HaveOccurred())
		Expect(mget.Val()).To(Equal([]interface{}{"a"}))

		mGet := client.MGet(ctx, "key2")
		Expect(mGet.Err()).NotTo(HaveOccurred())
		Expect(mGet.Val()).To(Equal([]interface{}{"b"}))

		mGet1 := client.MGet(ctx, "key3")
		Expect(mGet1.Err()).NotTo(HaveOccurred())
		Expect(mGet1.Val()).To(Equal([]interface{}{"c"}))

		mGet2 := client.MGet(ctx, "key4")
		Expect(mGet2.Err()).NotTo(HaveOccurred())
		Expect(mGet2.Val()).To(Equal([]interface{}{"d"}))

		mGet3 := client.MGet(ctx, "key1", "key2", "key3", "key4")
		Expect(mGet3.Err()).NotTo(HaveOccurred())
		Expect(mGet3.Val()).To(Equal([]interface{}{"a", "b", "c", "d"}))

		Expect(client.TTL(ctx, "key1").Val()).NotTo(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key2").Val()).NotTo(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key3").Val()).NotTo(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key4").Val()).NotTo(Equal(time.Duration(-2)))

		time.Sleep(4 * time.Second)

		Expect(client.TTL(ctx, "key1").Val()).To(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key2").Val()).To(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key3").Val()).To(Equal(time.Duration(-2)))
		Expect(client.TTL(ctx, "key4").Val()).To(Equal(time.Duration(-2)))

		mGet4 := client.MGet(ctx, "key1", "key2", "key3", "key4")
		Expect(mGet4.Err()).NotTo(HaveOccurred())
		Expect(mGet4.Val()).To(Equal([]interface{}{nil, nil, nil, nil}))
	})
})
