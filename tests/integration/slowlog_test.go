package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func isBetween(first, second, target int64) bool {
	return first <= target && second >= target
}

var _ = Describe("Slowlog Commands", func() {
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

	Describe("SlowLog", func() {
		It("returns slow query result", func() {
			const key = "slowlog-log-slower-than"

			old := client.ConfigGet(ctx, key).Val()
			client.ConfigSet(ctx, key, "0")
			defer client.ConfigSet(ctx, key, old[key])

			err := client.Do(ctx, "slowlog", "reset").Err()
			Expect(err).NotTo(HaveOccurred())

			client.Set(ctx, "test", "true", 0)

			result, err := client.SlowLogGet(ctx, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).NotTo(BeZero())
		})
		It("Should be able to handle the situation when the log is full correctly", func() {
			const key1 = "slowlog-log-slower-than"
			old := client.ConfigGet(ctx, key1).Val()
			defer Expect(client.ConfigSet(ctx, key1, old[key1]).Err()).NotTo(HaveOccurred())
			client.ConfigSet(ctx, key1, "0")

			const key2 = "slowlog-max-len"
			oldMaxLen := client.ConfigGet(ctx, key2).Val()
			defer Expect(client.ConfigSet(ctx, key2, oldMaxLen[key2]).Err()).NotTo(HaveOccurred())
			client.ConfigSet(ctx, key2, "10")

			for i := 0; i < 20; i++ {
				Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
			}

			result, err := client.SlowLogGet(ctx, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(Equal(int(10)))
		})
		It("Make sure that the returned timestamp is correct.", func() {
			const key1 = "slowlog-log-slower-than"
			old := client.ConfigGet(ctx, key1).Val()
			defer Expect(client.ConfigSet(ctx, key1, old[key1]).Err()).NotTo(HaveOccurred())
			client.ConfigSet(ctx, key1, "0")
			time1 := time.Now()
			Expect(client.Do(ctx, "SLOWLOG", "reset").Val()).To(Equal("OK"))

			time.Sleep(200 * time.Millisecond)
			for i := 0; i < 15; i++ {
				Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
			}
			result, err := client.SlowLogGet(ctx, 10).Result()
			time.Sleep(200 * time.Millisecond)
			time2 := time.Now()
			Expect(err).NotTo(HaveOccurred())

			for i := 0; i < 10; i++ {
				Expect(result[i].Time.Unix()).To(BeNumerically("~", 0, 9999999999))
				Expect(isBetween(time1.Unix(), time2.Unix(), result[i].Time.Unix())).To(Equal(true))
			}
		})
	})

})
