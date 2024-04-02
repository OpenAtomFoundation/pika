package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func SlotMigrateEnv(ctx context.Context, clientMaster, clientSlave *redis.Client) {
	r := clientSlave.Do(ctx, "slaveof", "no", "one")
	Expect(r.Err()).NotTo(HaveOccurred())
	Expect(r.Val()).To(Equal("OK"))
	Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	Expect(clientMaster.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
	Expect(clientSlave.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
}

var _ = Describe("SlotMigrate test", func() {
	ctx := context.TODO()
	var clientSlave *redis.Client
	var clientMaster *redis.Client
	BeforeEach(func() {
		clientMaster = redis.NewClient(PikaOption(MASTERADDR))
		clientSlave = redis.NewClient(PikaOption(SLAVEADDR))
		SlotMigrateEnv(ctx, clientMaster, clientSlave)
		if GlobalBefore != nil {
			GlobalBefore(ctx, clientSlave)
			GlobalBefore(ctx, clientMaster)
		}
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(clientMaster.Do(ctx, "config", "set", "slotmigrate", "no").Err()).NotTo(HaveOccurred())
		Expect(clientSlave.Do(ctx, "config", "set", "slotmigrate", "no").Err()).NotTo(HaveOccurred())
		Expect(clientMaster.Close()).NotTo(HaveOccurred())
		Expect(clientSlave.Close()).NotTo(HaveOccurred())
	})

	It("should SlotsInfo", func() {
		set := clientMaster.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		slotsinfo := clientMaster.Do(ctx, "slotsinfo")
		Expect(slotsinfo.Val()).NotTo(Equal("OK"))
	})

	It("should SlotsCleanup", func() {
		set := clientMaster.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsCleanup := clientMaster.Do(ctx, "SlotsCleanup", "80", "380", "490")
		Expect(SlotsCleanup.Err()).NotTo(HaveOccurred())

		time.Sleep(3 * time.Second)

		n1, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(0)))

		Get := clientMaster.Get(ctx, "key1")
		Expect(Get.Val()).To(Equal(""))

		Get1 := clientMaster.Get(ctx, "key2")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := clientMaster.Get(ctx, "key3")
		Expect(Get2.Val()).To(Equal(""))
	})

	It("should SlotsScan", func() {
		set := clientMaster.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsScan := clientMaster.Do(ctx, "SlotsScan", "80", "0", "COUNT", "10")
		Expect(SlotsScan.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey1"}}))

		SlotsScan1 := clientMaster.Do(ctx, "SlotsScan", "490", "0", "COUNT", "10")
		Expect(SlotsScan1.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey2"}}))

		SlotsScan2 := clientMaster.Do(ctx, "SlotsScan", "380", "0", "COUNT", "10")
		Expect(SlotsScan2.Val()).To(Equal([]interface{}{"0", []interface{}{"kkey3"}}))
	})

	It("should SlotsHashKey", func() {
		set := clientMaster.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		slotshashkey := clientMaster.Do(ctx, "slotshashkey", "key1")
		Expect(slotshashkey.Val()).To(Equal([]interface{}{int64(80)}))

		slotshashkey1 := clientMaster.Do(ctx, "slotshashkey", "key2")
		Expect(slotshashkey1.Val()).To(Equal([]interface{}{int64(490)}))

		slotshashkey2 := clientMaster.Do(ctx, "slotshashkey", "key3")
		Expect(slotshashkey2.Val()).To(Equal([]interface{}{int64(380)}))
	})

	It("should SlotsMgrtTagOne", func() {
		set := clientMaster.Set(ctx, "key1", "a", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2", "b", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3", "c", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1", "key2", "key3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		Expect(clientSlave.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
		set4 := clientMaster.Set(ctx, "x", "y", 0)
		Expect(set4.Err()).NotTo(HaveOccurred())
		Expect(set4.Val()).To(Equal("OK"))

		SlotsMgrtTagOne := clientMaster.Do(ctx, "SLOTSMGRTTAGONE", "127.0.0.1", "9231", "5000", "key1")
		Expect(SlotsMgrtTagOne.Val()).NotTo(Equal(int64(0)))
		SlotsMgrtTagOne1 := clientMaster.Do(ctx, "SLOTSMGRTTAGONE", "127.0.0.1", "9231", "5000", "key2")
		Expect(SlotsMgrtTagOne1.Val()).To(Equal(int64(1)))

		Get1 := clientMaster.Get(ctx, "key2")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := clientSlave.Get(ctx, "key2")
		Expect(Get2.Val()).To(Equal("b"))
	})

	It("should SlotsMgrtTagSlot", func() {
		set := clientMaster.Set(ctx, "key1tag1", "value1", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2tag2", "value2", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3tag3", "value3", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1tag1", "key2tag2", "key3tag3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsMgrtTagOne := clientMaster.Do(ctx, "SLOTSMGRTTAGSLOT", "127.0.0.1", "9231", "5000", "277")
		Expect(SlotsMgrtTagOne.Val()).To(Equal([]interface{}{int64(1), int64(0)}))

		SlotsMgrtTagOne1 := clientMaster.Do(ctx, "SLOTSMGRTTAGSLOT", "127.0.0.1", "9231", "5000", "51")
		Expect(SlotsMgrtTagOne1.Val()).To(Equal([]interface{}{int64(1), int64(0)}))

		SlotsMgrtTagOne2 := clientMaster.Do(ctx, "SLOTSMGRTTAGSLOT", "127.0.0.1", "9231", "5000", "639")
		Expect(SlotsMgrtTagOne2.Val()).To(Equal([]interface{}{int64(1), int64(0)}))

		Get1 := clientMaster.Get(ctx, "key1tag1")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := clientSlave.Get(ctx, "key1tag1")
		Expect(Get2.Val()).To(Equal("value1"))

		n1, err := clientMaster.Exists(ctx, "key1tag1", "key2tag2", "key3tag3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(0)))
	})

	It("should SlotsMgrtTagOne", func() {
		set := clientMaster.Set(ctx, "a{tag}", "100", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "b{tag}", "100", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "c{tag}", "100", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "a{tag}", "b{tag}", "c{tag}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		SlotsMgrtTagOne := clientMaster.Do(ctx, "SlotsMgrtTagOne", "127.0.0.1", "9231", "5000", "a{tag}")
		Expect(SlotsMgrtTagOne.Val()).To(Equal(int64(3)))

		Get1 := clientMaster.Get(ctx, "a{tag}")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := clientSlave.Get(ctx, "a{tag}")
		Expect(Get2.Val()).To(Equal("100"))

		Get3 := clientMaster.Get(ctx, "b{tag}")
		Expect(Get3.Val()).To(Equal(""))

		Get4 := clientSlave.Get(ctx, "b{tag}")
		Expect(Get4.Val()).To(Equal("100"))

		n1, err := clientMaster.Exists(ctx, "a{tag}", "b{tag}", "c{tag}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(0)))
	})

	It("should SlotsMgrtTagSlotAsync", func() {
		set := clientMaster.Set(ctx, "key1tag1", "value1", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		set2 := clientMaster.Set(ctx, "key2tag2", "value2", 0)
		Expect(set2.Err()).NotTo(HaveOccurred())
		Expect(set2.Val()).To(Equal("OK"))

		set3 := clientMaster.Set(ctx, "key3tag3", "value3", 0)
		Expect(set3.Err()).NotTo(HaveOccurred())
		Expect(set3.Val()).To(Equal("OK"))

		n, err := clientMaster.Exists(ctx, "key1tag1", "key2tag2", "key3tag3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		Expect(clientSlave.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())
		Expect(clientMaster.Do(ctx, "config", "set", "slotmigrate", "yes").Err()).NotTo(HaveOccurred())

		SlotsMgrtTagOne := clientMaster.Do(ctx, "slotsmgrttagslot-async", "127.0.0.1", "9231", "5000", "200", "33554432", "51", "1024")
		Expect(SlotsMgrtTagOne.Val()).To(Equal([]interface{}{int64(0), int64(1)}))

		SlotsMgrtTagOne1 := clientMaster.Do(ctx, "slotsmgrttagslot-async", "127.0.0.1", "9231", "5000", "200", "33554432", "277", "1024")
		Expect(SlotsMgrtTagOne1.Val()).To(Equal([]interface{}{int64(0), int64(1)}))

		SlotsMgrtTagOne2 := clientMaster.Do(ctx, "slotsmgrttagslot-async", "127.0.0.1", "9231", "5000", "200", "33554432", "639", "1024")
		Expect(SlotsMgrtTagOne2.Val()).To(Equal([]interface{}{int64(0), int64(1)}))

		Get1 := clientMaster.Get(ctx, "key1tag1")
		Expect(Get1.Val()).To(Equal(""))

		Get2 := clientSlave.Get(ctx, "key1tag1")
		Expect(Get2.Val()).To(Equal("value1"))

		n1, err := clientMaster.Exists(ctx, "key1tag1", "key2tag2", "key3tag3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n1).To(Equal(int64(0)))
	})
})
