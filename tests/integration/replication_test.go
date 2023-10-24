package pika_integration

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func cleanEnv(ctx context.Context, clientMaster, clientSlave *redis.Client) {
	r := clientSlave.Do(ctx, "slaveof", "no", "one")
	Expect(r.Err()).NotTo(HaveOccurred())
	Expect(r.Val()).To(Equal("OK"))
	r = clientSlave.Do(ctx, "clearreplicationid")
	r = clientMaster.Do(ctx, "clearreplicationid")
	//cmd := exec.Command("rm", "-rf", "/home/runner/work/pika/pika/dump")
	cmd := exec.Command("rm", "-rf", "/Users/dingxiaoshuai/pika_new/dump")

	errr := cmd.Run()
	if errr != nil {
		fmt.Println("remove dump fail!")
	}
}

func trySlave(ctx context.Context, clientSlave *redis.Client) bool {
	Expect(clientSlave.Do(ctx, "slaveof", "127.0.0.1", "9221").Val()).To(Equal("OK"))
	infoRes := clientSlave.Info(ctx, "replication")
	Expect(infoRes.Err()).NotTo(HaveOccurred())
	Expect(infoRes.Val()).To(ContainSubstring("role:slave"))
	var count = 0
	for {
		infoRes = clientSlave.Info(ctx, "replication")
		Expect(infoRes.Err()).NotTo(HaveOccurred())
		count++
		if strings.Contains(infoRes.Val(), "master_link_status:up") {
			return true
		} else if count > 100 {
			return false
		}
		time.Sleep(1 * time.Second)
	}
}

var _ = FDescribe("shuould replication ", func() {
	Describe("all replication test", func() {
		ctx := context.TODO()
		var clientSlave *redis.Client
		var clientMaster *redis.Client

		BeforeEach(func() {
			clientMaster = redis.NewClient(pikaOptions1())
			clientSlave = redis.NewClient(pikaOptions2())
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			time.Sleep(2 * time.Second)
		})
		AfterEach(func() {
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.Close()).NotTo(HaveOccurred())
			Expect(clientMaster.Close()).NotTo(HaveOccurred())
		})
		It("Let The slave become a replica of The master ", func() {
			infoRes := clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))

			res := clientMaster.Set(ctx, "key", "value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(Equal("OK"))
			res = clientMaster.Set(ctx, "string", "hello", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(Equal("OK"))
			Lres := clientMaster.LPush(ctx, "myList", "one", "two", "three")
			Expect(Lres.Err()).NotTo(HaveOccurred())
			Expect(Lres.Val()).To(Equal(int64(3)))
			Sres := clientMaster.SAdd(ctx, "mySet", "one", "two", "three")
			Expect(Sres.Err()).NotTo(HaveOccurred())
			Expect(Sres.Val()).To(Equal(int64(3)))
			Hres := clientMaster.HSet(ctx, "myHash", "key", "value")
			Expect(Hres.Err()).NotTo(HaveOccurred())
			Expect(Hres.Val()).To(Equal(int64(1)))
			var count = 0
			for {
				res := trySlave(ctx, clientSlave)
				if res {
					break
				} else if count > 3 {
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

			kget := clientSlave.Get(ctx, "key")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("value"))

			kget = clientSlave.Get(ctx, "string")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("hello"))

			slaveLrange := clientSlave.LRange(ctx, "myList", 0, -1)
			Expect(slaveLrange.Err()).NotTo(HaveOccurred())
			masterLrange := clientMaster.LRange(ctx, "myList", 0, -1)
			Expect(masterLrange.Err()).NotTo(HaveOccurred())
			Expect(slaveLrange).To(Equal(masterLrange))

			slaveSmem := clientSlave.SMembers(ctx, "mySet")
			Expect(slaveSmem.Err()).NotTo(HaveOccurred())
			masterSmem := clientMaster.SMembers(ctx, "mySet")
			Expect(masterSmem.Err()).NotTo(HaveOccurred())
			Expect(slaveSmem.Val()).To(Equal(masterSmem.Val()))

			slaveHget := clientSlave.HGet(ctx, "myHash", "key")
			Expect(slaveHget.Err()).NotTo(HaveOccurred())
			Expect(slaveHget.Val()).To(Equal("value"))

			slaveWrite := clientSlave.Set(ctx, "foo", "bar", 0)
			Expect(slaveWrite.Err()).To(MatchError("ERR Server in read-only"))

			res = clientMaster.Set(ctx, "Newstring", "NewHello", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(Equal("OK"))
			Lres = clientMaster.LPush(ctx, "myList", "Hello")
			Expect(Lres.Err()).NotTo(HaveOccurred())
			Expect(Lres.Val()).To(Equal(int64(4)))
			Sres = clientMaster.SAdd(ctx, "mySet", "Hello")
			Expect(Sres.Err()).NotTo(HaveOccurred())
			Expect(Sres.Val()).To(Equal(int64(1)))
			Hres = clientMaster.HSet(ctx, "myHash", "key2", "value2")
			Expect(Hres.Err()).NotTo(HaveOccurred())
			Expect(Hres.Val()).To(Equal(int64(1)))
			time.Sleep(10 * time.Second)

			kget = clientSlave.Get(ctx, "Newstring")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("NewHello"))

			slaveLrange = clientSlave.LRange(ctx, "myList", 0, -1)
			Expect(slaveLrange.Err()).NotTo(HaveOccurred())
			masterLrange = clientMaster.LRange(ctx, "myList", 0, -1)
			Expect(masterLrange.Err()).NotTo(HaveOccurred())
			Expect(slaveLrange).To(Equal(masterLrange))

			slaveSmem = clientSlave.SMembers(ctx, "mySet")
			Expect(slaveSmem.Err()).NotTo(HaveOccurred())
			masterSmem = clientMaster.SMembers(ctx, "mySet")
			Expect(masterSmem.Err()).NotTo(HaveOccurred())
			Expect(slaveSmem.Val()).To(Equal(masterSmem.Val()))

			slaveHget = clientSlave.HGet(ctx, "myHash", "key2")
			Expect(slaveHget.Err()).NotTo(HaveOccurred())
			Expect(slaveHget.Val()).To(Equal("value2"))

			noOneRes := clientSlave.Do(ctx, "slaveof", "no", "one")
			Expect(noOneRes.Err()).NotTo(HaveOccurred())
			Expect(noOneRes.Val()).To(Equal("OK"))
			Expect(clientSlave.Do(ctx, "clearreplicationid").Err()).NotTo(HaveOccurred())

			infoRes = clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))

			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))

			res = clientMaster.Set(ctx, "c_key", "c_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(Equal("OK"))
			res = clientMaster.Set(ctx, "c_key1", "c_value1", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(Equal("OK"))

			kget = clientMaster.Get(ctx, "c_key")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("c_value"))

			kget = clientMaster.Get(ctx, "c_key1")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("c_value1"))

			dres := clientMaster.Del(ctx, "c_key1")
			Expect(dres.Err()).NotTo(HaveOccurred())
			Expect(dres.Val()).To(Equal(int64(1)))

		})

		It("slaveof itself should return err", func() {
			infoRes := clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			Expect(clientSlave.Do(ctx, "slaveof", "127.0.0.1", "9231").Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))
		})

		It("test slaveof with localhost&port", func() {
			infoRes := clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			infoRes = clientMaster.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:master"))
			Expect(clientSlave.Do(ctx, "slaveof", "localhost", "9221").Val()).To(Equal("OK"))
			infoRes = clientSlave.Info(ctx, "replication")
			Expect(infoRes.Err()).NotTo(HaveOccurred())
			Expect(infoRes.Val()).To(ContainSubstring("role:slave"))
			var count = 0
			for {
				res := trySlave(ctx, clientSlave)
				if res {
					break
				} else if count > 3 {
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
		})

	})

})
