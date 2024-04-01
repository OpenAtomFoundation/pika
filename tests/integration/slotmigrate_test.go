package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

/*func SlotMigrateEnv(ctx context.Context, clientMaster, clientSlave *redis.Client) {
	r := clientSlave.Do(ctx, "slaveof", "no", "one")
	Expect(r.Err()).NotTo(HaveOccurred())
	Expect(r.Val()).To(Equal("OK"))
	Expect(clientMaster.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
	Expect(clientSlave.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
}*/

var _ = Describe("Slowlog Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
		if GlobalBefore != nil {
			GlobalBefore(ctx, client)
		}
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Do(ctx, "config", "set", "slotmigrate", "no").Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
		//Expect(clientSlave.Close()).NotTo(HaveOccurred())
	})

	It("should SlotsInfo", func() {
		set := client.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		slotsinfo := client.Do(ctx, "slotsinfo")
		Expect(slotsinfo.Val()).NotTo(Equal("OK"))
	})

	It("should SlotsCleanup", func() {
		set := client.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsCleanup := client.Do(ctx, "SlotsCleanup", "80", "380", "490")
		Expect(SlotsCleanup.Err()).NotTo(HaveOccurred())

		time.Sleep(3 * time.Second)

		n1, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(0)))

		Get := client.Get(ctx, "key1")
		Expect(Get.Val()).To(Equal(""))

		Get1 := client.Get(ctx, "key2")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := client.Get(ctx, "key3")
		Expect(Get2.Val()).To(Equal(""))
	})

	It("should SlotsScan", func() {
		set := client.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsScan := client.Do(ctx, "SlotsScan", "80", "0", "COUNT", "10")
		Expect(SlotsScan.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey1"}}))

		SlotsScan1 := client.Do(ctx, "SlotsScan", "490", "0", "COUNT", "10")
		Expect(SlotsScan1.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey2"}}))

		SlotsScan2 := client.Do(ctx, "SlotsScan", "380", "0", "COUNT", "10")
		Expect(SlotsScan2.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey3"}}))
	})

	It("should SlotsHashKey", func() {
		set := client.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := client.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := client.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := client.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		slotshashkey := client.Do(ctx, "slotshashkey", "key1")
		Expect(slotshashkey.Val()).To(Equal([]interface{}{int64(80)}))

		slotshashkey1 := client.Do(ctx, "slotshashkey", "key2")
		Expect(slotshashkey1.Val()).To(Equal([]interface{}{int64(490)}))

		slotshashkey2 := client.Do(ctx, "slotshashkey", "key3")
		Expect(slotshashkey2.Val()).To(Equal([]interface{}{int64(380)}))
	})
})
