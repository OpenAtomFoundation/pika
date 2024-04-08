package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("admin test", func() {
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

	It("should info", func() {
		set := client.Set(ctx, "key", "foobar", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		bitCount := client.BitCount(ctx, "key", nil)
		Expect(bitCount.Err()).NotTo(HaveOccurred())
		Expect(bitCount.Val()).To(Equal(int64(26)))

		set1 := client.Set(ctx, "key1", "value1", 0)
		Expect(set1.Err()).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key2", "value2", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key3", "value3", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		set4 := client.Set(ctx, "key4", "value4", 0)
		Expect(set4.Err()).NotTo(HaveOccurred())
		Expect(set4.Val()).To(Equal("OK"))

		lPush := client.LPush(ctx, "key2", "b")
		Expect(lPush.Err()).NotTo(HaveOccurred())

		sAdd := client.SAdd(ctx, "key3", "c")
		Expect(sAdd.Err()).NotTo(HaveOccurred())
		Expect(sAdd.Val()).To(Equal(int64(1)))

		infokeyspace := client.Info(ctx, "keyspace", "1")
		Expect(infokeyspace.Err()).NotTo(HaveOccurred())
		Expect(infokeyspace.Val()).NotTo(Equal(""))

		start := time.Now()
		infoall := client.Info(ctx, "all")
		Expect(infoall.Err()).NotTo(HaveOccurred())
		Expect(infoall.Val()).NotTo(Equal(""))
		end := time.Now()
		duration := end.Sub(start)
		Expect(duration).To(BeNumerically("<", time.Second))

		start1 := time.Now()
		infoacache := client.Info(ctx)
		Expect(infoacache.Err()).NotTo(HaveOccurred())
		Expect(infoacache.Val()).NotTo(Equal(""))
		end1 := time.Now()
		duration1 := end1.Sub(start1)
		Expect(duration1).To(BeNumerically("<", time.Second))

		start2 := time.Now()
		info := client.Info(ctx, "cache")
		Expect(info.Err()).NotTo(HaveOccurred())
		Expect(info.Val()).NotTo(Equal(""))
		end2 := time.Now()
		duration2 := end2.Sub(start2)
		Expect(duration2).To(BeNumerically("<", time.Second))
	})

})
