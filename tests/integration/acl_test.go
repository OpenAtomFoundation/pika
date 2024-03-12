package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Acl test", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
	})

	AfterEach(func() {
	})

	It("has requirepass & userpass & blacklist", func() {
		client = redis.NewClient(PikaOption(ACLADDR_1))
		authRes := client.Do(ctx, "auth", "wrong!")
		Expect(authRes.Err()).To(MatchError("WRONGPASS invalid username-password pair or user is disabled."))

		// user:limit
		authRes = client.Do(ctx, "auth", "userpass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		limitRes := client.Do(ctx, "flushall")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushall' command"))

		limitRes = client.Do(ctx, "flushdb")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushdb' command"))

		// user:default
		authRes = client.Do(ctx, "auth", "requirepass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		adminRes := client.Do(ctx, "flushall")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))

		adminRes = client.Do(ctx, "flushdb")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))

	})
	It("has requirepass & blacklist", func() {
		client := redis.NewClient(PikaOption(ACLADDR_2))

		// user:limit
		authRes := client.Do(ctx, "auth", "anypass")
		Expect(authRes.Err()).NotTo(HaveOccurred())

		limitRes := client.Do(ctx, "flushall")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushall' command"))

		limitRes = client.Do(ctx, "flushdb")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushdb' command"))

		// user:default
		authRes = client.Do(ctx, "auth", "requirepass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		adminRes := client.Do(ctx, "flushall")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))

		adminRes = client.Do(ctx, "flushdb")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))

	})
	It("has other acl user", func() {
		client := redis.NewClient(PikaOption(ACLADDR_3))

		authRes := client.Do(ctx, "auth", "wrong!")
		Expect(authRes.Err()).To(MatchError("WRONGPASS invalid username-password pair or user is disabled."))

		// user:limit
		authRes = client.Do(ctx, "auth", "userpass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		limitRes := client.Do(ctx, "flushall")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushall' command"))

		limitRes = client.Do(ctx, "flushdb")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushdb' command"))

		// user:limit
		authRes = client.Do(ctx, "auth", "limitpass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		limitRes = client.Do(ctx, "flushall")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushall' command"))

		limitRes = client.Do(ctx, "flushdb")
		Expect(limitRes.Err()).To(MatchError("NOPERM this user has no permissions to run the 'flushdb' command"))

		// user:default
		authRes = client.Do(ctx, "auth", "requirepass")
		Expect(authRes.Err()).NotTo(HaveOccurred())
		Expect(authRes.Val()).To(Equal("OK"))

		adminRes := client.Do(ctx, "flushall")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))

		adminRes = client.Do(ctx, "flushdb")
		Expect(adminRes.Err()).NotTo(HaveOccurred())
		Expect(adminRes.Val()).To(Equal("OK"))
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
