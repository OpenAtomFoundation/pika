package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Rename Command test", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(RenameADDR))
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should 360FlushDB", func() {
		set := client.Set(ctx, "key", "foobar", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		bitCount := client.BitCount(ctx, "key", nil)
		Expect(bitCount.Err()).NotTo(HaveOccurred())
		Expect(bitCount.Val()).To(Equal(int64(26)))
		_, err := client.Do(ctx, "360flushdb").Result()
		Expect(err).NotTo(HaveOccurred())
		r := client.Do(ctx, "360flushdb")
		Expect(r.Val()).To(Equal("OK"))
		n, err := client.Exists(ctx, "key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(0)))
		r = client.Do(ctx, "flushdb")
		Expect(r.Val()).NotTo(Equal("OK"))
	})

})
