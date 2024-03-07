package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Acl test", func() {
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

	It("should acl dryrun", func() {
		dryRun := client.ACLDryRun(ctx, "default", "get", "randomKey")

		Expect(dryRun.Err()).NotTo(HaveOccurred())
		Expect(dryRun.Val()).To(Equal("OK"))
	})

	It("should ACL LOG RESET", Label("NonRedisEnterprise"), func() {
		// Call ACL LOG RESET
		resetCmd := client.ACLLogReset(ctx)
		Expect(resetCmd.Err()).NotTo(HaveOccurred())
		Expect(resetCmd.Val()).To(Equal("OK"))

		// Verify that the log is empty after the reset
		logEntries, err := client.ACLLog(ctx, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(logEntries)).To(Equal(0))
	})
})
