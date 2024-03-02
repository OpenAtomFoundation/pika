package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Hyperloglog Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("hyperloglog", func() {
		It("should PFMerge", func() {
			pfAdd := client.PFAdd(ctx, "hll1", "1", "2", "3", "4", "5")
			Expect(pfAdd.Err()).NotTo(HaveOccurred())

			pfCount := client.PFCount(ctx, "hll1")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(5)))

			pfAdd = client.PFAdd(ctx, "hll2", "a", "b", "c", "d", "e")
			Expect(pfAdd.Err()).NotTo(HaveOccurred())

			pfMerge := client.PFMerge(ctx, "hllMerged", "hll1", "hll2")
			Expect(pfMerge.Err()).NotTo(HaveOccurred())

			pfCount = client.PFCount(ctx, "hllMerged")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(10)))

			pfCount = client.PFCount(ctx, "hll1", "hll2")
			Expect(pfCount.Err()).NotTo(HaveOccurred())
			Expect(pfCount.Val()).To(Equal(int64(10)))
		})
	})
})
