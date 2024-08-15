package pika_integration

import (
	"context"
	"log"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("should replication rename", func() {
	Describe("all replication rename test", func() {
		ctx := context.TODO()
		var clientSlave *redis.Client
		var clientMaster *redis.Client

		BeforeEach(func() {
			clientMaster = redis.NewClient(PikaOption(MASTERRENAMEADDR))
			clientSlave = redis.NewClient(PikaOption(SLAVERENAMEADDR))
			cleanEnv(ctx, clientMaster, clientSlave)
			if GlobalBefore != nil {
				GlobalBefore(ctx, clientMaster)
				GlobalBefore(ctx, clientSlave)
			}
		})
		AfterEach(func() {
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.Close()).NotTo(HaveOccurred())
			Expect(clientMaster.Close()).NotTo(HaveOccurred())
			log.Println("Replication test case done")
		})

		It("Let The slave become a replica of The master ", func() {
			infoRes := clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			Expect(clientSlave.Do(ctx, "slaveof", LOCALHOST, SLAVERENAMEPORT).Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

			var count = 0
			for {
				res := trySlave(ctx, clientSlave, LOCALHOST, MASTERRENAMEPORT)
				if res {
					break
				} else if count > 4 {
					break
				} else {
					cleanEnv(ctx, clientMaster, clientSlave)
					count++
				}
			}

			infoRes = clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("master_link_status:up"))

			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("connected_slaves:1"))

			slaveWrite := clientSlave.Set(ctx, "foo", "bar", 0)
			Expect(slaveWrite.Err()).To(MatchError("ERR READONLY You can't write against a read only replica."))
			log.Println("Replication rename test 1 start")
			set := clientMaster.Set(ctx, "x", "y", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))
			set1 := clientMaster.Set(ctx, "a", "b", 0)
			Expect(set1.Err()).NotTo(HaveOccurred())
			Expect(set1.Val()).To(Equal("OK"))
			r1 := clientMaster.Do(ctx, "flushdb")
			Expect(r1.Val()).NotTo(Equal("OK"))
			time.Sleep(3 * time.Second)
			Expect(clientMaster.Do(ctx, "360flushdb").Err()).NotTo(HaveOccurred())
			Eventually(func() error {
				return clientMaster.Get(ctx, "x").Err()
			}, "60s", "100ms").Should(Equal(redis.Nil))
			Eventually(func() error {
				return clientSlave.Get(ctx, "x").Err()
			}, "60s", "100ms").Should(Equal(redis.Nil))
			log.Println("Replication rename test 1 success")

		})
	})
})
