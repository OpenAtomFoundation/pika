package pika_integration

import (
	"context"
	"fmt"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Csanning Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		GlobalBefore(ctx, client)
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("scanning", func() {
		It("should Scan", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.Scan(ctx, 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should ScanType", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ScanType(ctx, 0, "", 0, "").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should SScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.SAdd(ctx, "myset", fmt.Sprintf("member%d", i))
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.SScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should HScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.HScan(ctx, "myhash", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should ZScan", func() {
			for i := 0; i < 1000; i++ {
				err := client.ZAdd(ctx, "myset", redis.Z{
					Score:  float64(i),
					Member: fmt.Sprintf("member%d", i),
				}).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ZScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})
	})
})
