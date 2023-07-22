package pika_integration

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Slowlog Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(pikarOptions1())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("SlowLogGet", func() {
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
	})
})
