package pika_integration

import (
	"context"
	"fmt"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"os/exec"
	"strings"
	"time"
)

var _ = FDescribe("shuould replication ", func() {
	Describe("all replication test", func() {
		//  在这里定义两个连接
		ctx := context.TODO()
		var clientSlave *redis.Client
		var clientMaster *redis.Client

		BeforeEach(func() {
			clientMaster = redis.NewClient(pikaOptions1())
			clientSlave = redis.NewClient(pikaOptions2())
			r := clientSlave.Do(ctx, "slaveof", "no", "one")
			Expect(r.Err()).NotTo(HaveOccurred())
			Expect(r.Val()).To(Equal("OK"))
			r = clientSlave.Do(ctx, "clearreplicationid")
			r = clientMaster.Do(ctx, "clearreplicationid")
			cmd := exec.Command("rm", "-rf", "/home/runner/work/pika/pika/tests/integration/dump")
			errr := cmd.Run()
			if errr != nil {
				fmt.Println("删除dump文件失败")
			}
			Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			fmt.Println("检查flushdb是否干净")
			time.Sleep(2 * time.Second)
		})
		AfterEach(func() {
			Expect(clientSlave.Close()).NotTo(HaveOccurred())
			Expect(clientMaster.Close()).NotTo(HaveOccurred())
		})
		It("Let The slave become a replica of The master ", func() {
			//  检查没有进行主从前的信息是否正确
			info_res := clientSlave.Info(ctx, "replication")
			Expect(info_res.Err()).NotTo(HaveOccurred())
			Expect(info_res.Val()).To(ContainSubstring("role:master"))
			info_res = clientMaster.Info(ctx, "replication")
			Expect(info_res.Err()).NotTo(HaveOccurred())
			Expect(info_res.Val()).To(ContainSubstring("role:master"))
			Expect(clientSlave.Do(ctx, "slaveof", "127.0.0.1", "9231").Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

			//  向主节点插入数据
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

			// 主从
			Expect(clientSlave.Do(ctx, "slaveof", "127.0.0.1", "9221").Val()).To(Equal("OK"))
			info_res = clientSlave.Info(ctx, "replication")
			Expect(info_res.Err()).NotTo(HaveOccurred())
			Expect(info_res.Val()).To(ContainSubstring("role:slave"))
			var count = 0
			for {
				info_res = clientSlave.Info(ctx, "replication")
				count++
				if strings.Contains(info_res.Val(), "master_link_status:up") || count == 100 {
					break
				}
				time.Sleep(1 * time.Second)
			}
			//  主节点会有connected_slaves:1
			info_res = clientMaster.Info(ctx, "replication")
			Expect(info_res.Val()).To(ContainSubstring("connected_slaves:1"))

			//  检查全量同步是否完成
			kget := clientSlave.Get(ctx, "key")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("value"))

			kget = clientSlave.Get(ctx, "string")
			Expect(kget.Err()).NotTo(HaveOccurred())
			Expect(kget.Val()).To(Equal("hello"))

			slave_lrange := clientSlave.LRange(ctx, "myList", 0, -1)
			Expect(slave_lrange.Err()).NotTo(HaveOccurred())
			master_lrange := clientMaster.LRange(ctx, "myList", 0, -1)
			Expect(master_lrange.Err()).NotTo(HaveOccurred())
			Expect(slave_lrange).To(Equal(master_lrange))

			slave_smem := clientSlave.SMembers(ctx, "mySet")
			Expect(slave_smem.Err()).NotTo(HaveOccurred())
			master_smem := clientMaster.SMembers(ctx, "mySet")
			Expect(master_smem.Err()).NotTo(HaveOccurred())
			Expect(slave_smem.Val()).To(Equal(master_smem.Val()))

			slave_hget := clientSlave.HGet(ctx, "myHash", "key")
			Expect(slave_hget.Err()).NotTo(HaveOccurred())
			Expect(slave_hget.Val()).To(Equal("value"))

			slave_write := clientSlave.Set(ctx, "foo", "bar", 0)
			Expect(slave_write.Err()).To(MatchError("ERR Server in read-only"))

			//  测试增量同步, 在主节点中继续增加数据
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

			slave_lrange = clientSlave.LRange(ctx, "myList", 0, -1)
			Expect(slave_lrange.Err()).NotTo(HaveOccurred())
			master_lrange = clientMaster.LRange(ctx, "myList", 0, -1)
			Expect(master_lrange.Err()).NotTo(HaveOccurred())
			Expect(slave_lrange).To(Equal(master_lrange))

			slave_smem = clientSlave.SMembers(ctx, "mySet")
			Expect(slave_smem.Err()).NotTo(HaveOccurred())
			master_smem = clientMaster.SMembers(ctx, "mySet")
			Expect(master_smem.Err()).NotTo(HaveOccurred())
			Expect(slave_smem.Val()).To(Equal(master_smem.Val()))

			slave_hget = clientSlave.HGet(ctx, "myHash", "key2")
			Expect(slave_hget.Err()).NotTo(HaveOccurred())
			Expect(slave_hget.Val()).To(Equal("value2"))

			//  测试slavoof no one 是否正常
			no_one_res := clientSlave.Do(ctx, "slaveof", "no", "one")
			Expect(no_one_res.Err()).NotTo(HaveOccurred())
			Expect(no_one_res.Val()).To(Equal("OK"))
			Expect(clientSlave.Do(ctx, "clearreplicationid").Err()).NotTo(HaveOccurred())

			info_res = clientSlave.Info(ctx, "replication")
			Expect(info_res.Err()).NotTo(HaveOccurred())
			Expect(info_res.Val()).To(ContainSubstring("role:master"))

			info_res = clientMaster.Info(ctx, "replication")
			Expect(info_res.Err()).NotTo(HaveOccurred())
			Expect(info_res.Val()).To(ContainSubstring("role:master"))

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

	})

})
