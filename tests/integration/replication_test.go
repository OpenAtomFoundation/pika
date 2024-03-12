package pika_integration

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

type command_func func(*context.Context, *redis.Client, *sync.WaitGroup)

func cleanEnv(ctx context.Context, clientMaster, clientSlave *redis.Client) {
	r := clientSlave.Do(ctx, "slaveof", "no", "one")
	Expect(r.Err()).NotTo(HaveOccurred())
	Expect(r.Val()).To(Equal("OK"))
	r = clientSlave.Do(ctx, "clearreplicationid")
	r = clientMaster.Do(ctx, "clearreplicationid")
	time.Sleep(1 * time.Second)
}

func trySlave(ctx context.Context, clientSlave *redis.Client, ip string, port string) bool {
	Expect(clientSlave.Do(ctx, "slaveof", ip, port).Val()).To(Equal("OK"))
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
		} else if count > 200 {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func randomInt(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}

func rpoplpushThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		letters1 := randomString(5)
		letters2 := randomString(5)
		letters3 := randomString(5)

		clientMaster.LPush(*ctx, "blist0", letters1)
		clientMaster.RPopLPush(*ctx, "blist0", "blist")
		clientMaster.LPush(*ctx, "blist", letters1, letters2, letters3)

		clientMaster.LPop(*ctx, "blist")
		clientMaster.RPop(*ctx, "blist")
		clientMaster.LPush(*ctx, "blist0", letters3)
		clientMaster.RPopLPush(*ctx, "blist0", "blist")
		clientMaster.RPush(*ctx, "blist", letters3, letters2, letters1)
		clientMaster.LPop(*ctx, "blist")
		clientMaster.LPush(*ctx, "blist0", letters2)
		clientMaster.RPopLPush(*ctx, "blist0", "blist")
		clientMaster.RPop(*ctx, "blist")
	}

}

func randomBitopThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		offset1 := randomInt(50)
		offset2 := randomInt(50)
		value1 := randomInt(1)
		value2 := randomInt(1)

		clientMaster.SetBit(*ctx, "bitkey1", int64(offset1), value1)
		clientMaster.SetBit(*ctx, "bitkey2", int64(offset1), value1)
		clientMaster.BitOpAnd(*ctx, "bitkey_out", "bitkey1", "bitkey2")
		clientMaster.SetBit(*ctx, "bitkey1", int64(offset1+offset2), value2)
		clientMaster.SetBit(*ctx, "bitkey2", int64(offset2), value2)
		clientMaster.BitOpOr(*ctx, "bitkey_out2", "bitkey1", "bitkey2")
	}

}

func randomSmoveThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	member := randomString(5)
	clientMaster.SAdd(*ctx, "sourceSet", member)
	clientMaster.SAdd(*ctx, "sourceSet", member)
	clientMaster.SAdd(*ctx, "sourceSet", member)
	clientMaster.SRem(*ctx, "destSet", member)
	clientMaster.SRem(*ctx, "destSet", member)
	clientMaster.SMove(*ctx, "sourceSet", "destSet", member)
}

func randomSdiffstoreThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "dest_set", randomString(5))
		clientMaster.SDiffStore(*ctx, "dest_set", "set1", "set2")
	}
}

func randomSinterstoreThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		member := randomString(5)
		member2 := randomString(5)
		member3 := randomString(5)
		member4 := randomString(5)
		member5 := randomString(5)
		member6 := randomString(5)
		clientMaster.SAdd(*ctx, "set1", member)
		clientMaster.SAdd(*ctx, "set2", member)
		clientMaster.SAdd(*ctx, "set1", member2)
		clientMaster.SAdd(*ctx, "set2", member2)
		clientMaster.SAdd(*ctx, "set1", member3)
		clientMaster.SAdd(*ctx, "set2", member3)
		clientMaster.SAdd(*ctx, "set1", member4)
		clientMaster.SAdd(*ctx, "set2", member4)
		clientMaster.SAdd(*ctx, "set1", member5)
		clientMaster.SAdd(*ctx, "set2", member5)
		clientMaster.SAdd(*ctx, "set1", member6)
		clientMaster.SAdd(*ctx, "set2", member6)
		clientMaster.SInterStore(*ctx, "dest_set", "set1", "set2")
	}
}

func test_del_replication(ctx *context.Context, clientMaster, clientSlave *redis.Client) {
	clientMaster.Del(*ctx, "blist0", "blist1", "blist2", "blist3")
	clientMaster.Del(*ctx, "blist100", "blist101", "blist102", "blist103")
	clientMaster.Del(*ctx, "blist0", "blist1", "blist2", "blist3")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.RPush(*ctx, "blist2", "v2")
	clientMaster.RPop(*ctx, "blist2")
	clientMaster.LPop(*ctx, "blist3")

	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.RPop(*ctx, "blist3")

	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.LPush(*ctx, "blist2", "v2")

	clientMaster.RPop(*ctx, "blist3")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.RPush(*ctx, "blist3", "v2")

	clientMaster.RPop(*ctx, "blist3")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.LPush(*ctx, "blist2", "v2")

	clientMaster.RPop(*ctx, "blist3")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.RPush(*ctx, "blist3", "v2")

	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.LPush(*ctx, "blist2", "v2")

	clientMaster.LPush(*ctx, "blist2", "v2")
	clientMaster.RPush(*ctx, "blist3", "v2")
	clientMaster.Del(*ctx, "blist1", "large", "blist2")

	clientMaster.RPush(*ctx, "blist1", "a", "latge", "c")
	clientMaster.RPush(*ctx, "blist2", "d", "latge", "f")

	clientMaster.LPop(*ctx, "blist1")
	clientMaster.RPop(*ctx, "blist1")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.RPop(*ctx, "blist2")

	clientMaster.Del(*ctx, "blist3")
	clientMaster.LPop(*ctx, "blist2")
	clientMaster.RPop(*ctx, "blist1")
	time.Sleep(15 * time.Second)

	for i := int64(0); i < clientMaster.LLen(*ctx, "blist1").Val(); i++ {
		Expect(clientMaster.LIndex(*ctx, "blist", i)).To(Equal(clientSlave.LIndex(*ctx, "blist", i)))
	}
	for i := int64(0); i < clientMaster.LLen(*ctx, "blist2").Val(); i++ {
		Expect(clientMaster.LIndex(*ctx, "blist2", i)).To(Equal(clientSlave.LIndex(*ctx, "blist2", i)))
	}
	for i := int64(0); i < clientMaster.LLen(*ctx, "blist3").Val(); i++ {
		Expect(clientMaster.LIndex(*ctx, "blist3", i)).To(Equal(clientSlave.LIndex(*ctx, "blist3", i)))
	}
	clientMaster.Del(*ctx, "blist100", "blist101", "blist102", "blist103")
	clientMaster.Del(*ctx, "blist0", "blist1", "blist2", "blist3")

}

func randomZunionstoreThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(10), randomString(5))
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(10), randomString(5))
		clientMaster.ZUnionStore(*ctx, "zset_out", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}})
		clientMaster.Do(*ctx, "zadd", "zset_out", randomInt(10), randomString(5))
	}
}

func randomZinterstoreThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		member := randomString(5)
		member2 := randomString(5)
		member3 := randomString(5)
		member4 := randomString(5)
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(5), member)
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(5), member)
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(5), member2)
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(5), member2)
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(5), member3)
		clientMaster.Do(*ctx, "zadd", "zset1", randomInt(5), member3)
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(5), member4)
		clientMaster.Do(*ctx, "zadd", "zset2", randomInt(5), member4)
		clientMaster.ZInterStore(*ctx, "zset_out", &redis.ZStore{Keys: []string{"zset1", "zset2"}, Weights: []float64{1, 1}})
	}
}

func randomSunionstroeThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SAdd(*ctx, "set1", randomString(5))
		clientMaster.SAdd(*ctx, "set2", randomString(5))
		clientMaster.SUnionStore(*ctx, "set_out", "set1", "set2")
	}
}

func randomXaddThread(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		message := map[string]interface{}{
			"field1": randomString(5),
			"field2": randomString(5),
		}
		clientMaster.XAdd(*ctx, &redis.XAddArgs{
			Stream: "mystream",
			Values: message,
		})
	}
}

func execute(ctx *context.Context, clientMaster *redis.Client, num_thread int, f command_func) {
	var wg sync.WaitGroup
	wg.Add(num_thread)
	for i := 1; i <= num_thread; i++ {
		go f(ctx, clientMaster, &wg)
	}
	wg.Wait()
	time.Sleep(10 * time.Second)
}

//func randomPfmergeThread(ctx *context.Context, clientMaster *redis.Client) {
//	clientMaster.PFAdd(*ctx, "hll1", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll2", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll2", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll1", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll2", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll1", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll2", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll1", randomString(5))
//	clientMaster.PFAdd(*ctx, "hll_out", randomString(5))
//	clientMaster.PFMerge(*ctx, "hll_out", "hll1", "hll2")
//	clientMaster.PFAdd(*ctx, "hll_out", randomString(5))
//}

func issueBLPopCheck(ctx *context.Context, client *redis.Client, list string, random_str string) {
	defer GinkgoRecover()
	bLPop := client.BLPop(*ctx, 0, "list0", "list1")
	Expect(bLPop.Err()).NotTo(HaveOccurred())
	Expect(bLPop.Val()).To(Equal([]string{list, random_str}))
}

func issueBRpopCheck(ctx *context.Context, client *redis.Client, list string, random_str string) {
	defer GinkgoRecover()
	bRPop := client.BRPop(*ctx, 0, "list0", "list1")
	Expect(bRPop.Err()).NotTo(HaveOccurred())
	Expect(bRPop.Val()).To(Equal([]string{list, random_str}))
}

func issuePushPopFrequency(ctx *context.Context, clientMaster *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	letters1 := randomString(5)
	letters2 := randomString(5)
	letters3 := randomString(5)
	letters4 := randomString(5)
	letters5 := randomString(5)
	letters6 := randomString(5)
	letters7 := randomString(5)
	letters8 := randomString(5)
	letters9 := randomString(5)

	clientMaster.LPush(*ctx, "blist0", letters1, letters2, letters3, letters4, letters5)
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")

	clientMaster.RPush(*ctx, "blist0", letters9, letters8, letters7, letters6, letters5)
	clientMaster.BRPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BRPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BRPop(*ctx, 1*time.Second, "blist0")

	clientMaster.RPush(*ctx, "blist0", letters7, letters8, letters9, letters1, letters2)
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")
	clientMaster.BLPop(*ctx, 1*time.Second, "blist0")
}

var _ = Describe("should replication ", func() {
	Describe("all replication test", func() {
		ctx := context.TODO()
		var clientSlave *redis.Client
		var clientMaster *redis.Client

		BeforeEach(func() {
			clientMaster = redis.NewClient(PikaOption(MASTERADDR))
			clientSlave = redis.NewClient(PikaOption(SLAVEADDR))
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			time.Sleep(3 * time.Second)
		})
		AfterEach(func() {
			cleanEnv(ctx, clientMaster, clientSlave)
			Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			time.Sleep(3 * time.Second)
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
			Expect(clientSlave.Do(ctx, "slaveof", LOCALHOST, SLAVEPORT).Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

			var count = 0
			for {
				res := trySlave(ctx, clientSlave, LOCALHOST, MASTERPORT)
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
			Expect(slaveWrite.Err()).To(MatchError("ERR Server in read-only"))

			log.Println("Replication test 1 start")
			err1 := clientMaster.SetEx(ctx, "key", "hello", 60*time.Second).Err()
			Expect(err1).NotTo(HaveOccurred())
			Eventually(func() error {
				return clientMaster.Get(ctx, "key").Err()
			}, "65s", "100ms").Should(Equal(redis.Nil))
			Eventually(func() error {
				return clientSlave.Get(ctx, "key").Err()
			}, "65s", "100ms").Should(Equal(redis.Nil))
			log.Println("Replication test 1 success")

			log.Println("Replication test 2 start")
			set := clientMaster.Set(ctx, "x", "y", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))
			set1 := clientMaster.Set(ctx, "a", "b", 0)
			Expect(set1.Err()).NotTo(HaveOccurred())
			Expect(set1.Val()).To(Equal("OK"))
			Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Eventually(func() error {
				return clientMaster.Get(ctx, "x").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
			Eventually(func() error {
				return clientSlave.Get(ctx, "x").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
			log.Println("Replication test 2 success")

			log.Println("rpoplpush test start")
			Expect(clientMaster.Del(ctx, "blist0", "blist1", "blist").Err()).NotTo(HaveOccurred())
			execute(&ctx, clientMaster, 4, rpoplpushThread)
			// TODO, the problem was not reproduced locally, record an issue first: https://github.com/OpenAtomFoundation/pika/issues/2492
			//for i := int64(0); i < clientMaster.LLen(ctx, "blist").Val(); i++ {
			//	Expect(clientMaster.LIndex(ctx, "blist", i)).To(Equal(clientSlave.LIndex(ctx, "blist", i)))
			//}
			Expect(clientMaster.Del(ctx, "blist0", "blist1", "blist").Err()).NotTo(HaveOccurred())
			log.Println("rpoplpush test success")

			log.Println("randomBitop test start")
			Expect(clientMaster.Del(ctx, "bitkey1", "bitkey2", "bitkey_out1", "bitkey_out2").Err()).NotTo(HaveOccurred())
			execute(&ctx, clientMaster, 4, randomBitopThread)
			master_key_out_count1 := clientMaster.Do(ctx, "bitcount", "bitkey_out1", 0, -1)
			slave_key_out_count1 := clientSlave.Do(ctx, "bitcount", "bitkey_out1", 0, -1)
			Expect(master_key_out_count1.Val()).To(Equal(slave_key_out_count1.Val()))
			master_key_out_count2 := clientMaster.Do(ctx, "bitcount", "bitkey_out2", 0, -1)
			slave_key_out_count2 := clientSlave.Do(ctx, "bitcount", "bitkey_out2", 0, -1)
			Expect(master_key_out_count2.Val()).To(Equal(slave_key_out_count2.Val()))
			Expect(clientMaster.Del(ctx, "bitkey1", "bitkey2", "bitkey_out1", "bitkey_out2").Err()).NotTo(HaveOccurred())
			log.Println("randomBitop test success")

			log.Println("randomSmove test start")
			Expect(clientMaster.Del(ctx, "sourceSet", "destSet").Err()).NotTo(HaveOccurred())
			execute(&ctx, clientMaster, 4, randomSmoveThread)
			master_source_set := clientMaster.SMembers(ctx, "sourceSet")
			Expect(master_source_set.Err()).NotTo(HaveOccurred())
			slave_source_set := clientSlave.SMembers(ctx, "sourceSet")
			Expect(slave_source_set.Err()).NotTo(HaveOccurred())
			Expect(master_source_set.Val()).To(Equal(slave_source_set.Val()))
			master_dest_set := clientMaster.SMembers(ctx, "destSet")
			Expect(master_dest_set.Err()).NotTo(HaveOccurred())
			slave_dest_set := clientSlave.SMembers(ctx, "destSet")
			Expect(slave_dest_set.Err()).NotTo(HaveOccurred())
			Expect(master_dest_set.Val()).To(Equal(slave_dest_set.Val()))
			Expect(clientMaster.Del(ctx, "sourceSet", "destSet").Err()).NotTo(HaveOccurred())
			test_del_replication(&ctx, clientMaster, clientSlave)
			log.Println("randomSmove test success")

			log.Println("randomSdiffstore test start")
			clientMaster.Del(ctx, "set1", "set2", "dest_set")
			execute(&ctx, clientMaster, 4, randomSdiffstoreThread)
			master_set1 := clientMaster.SMembers(ctx, "set1")
			Expect(master_set1.Err()).NotTo(HaveOccurred())
			slave_set1 := clientSlave.SMembers(ctx, "set1")
			Expect(slave_set1.Err()).NotTo(HaveOccurred())
			Expect(master_set1.Val()).To(Equal(slave_set1.Val()))
			master_set2 := clientMaster.SMembers(ctx, "set2")
			Expect(master_set2.Err()).NotTo(HaveOccurred())
			slave_set2 := clientSlave.SMembers(ctx, "set2")
			Expect(slave_set2.Err()).NotTo(HaveOccurred())
			Expect(master_set2.Val()).To(Equal(slave_set2.Val()))
			master_dest_store_set := clientMaster.SMembers(ctx, "dest_set")
			Expect(master_dest_store_set.Err()).NotTo(HaveOccurred())
			slave_dest_store_set := clientSlave.SMembers(ctx, "dest_set")
			Expect(slave_dest_store_set.Err()).NotTo(HaveOccurred())
			Expect(master_dest_store_set.Val()).To(Equal(slave_dest_store_set.Val()))
			clientMaster.Del(ctx, "set1", "set2", "dest_set")
			log.Println("randomSdiffstore test success")

			log.Println("randomSinterstore test start")
			clientMaster.Del(ctx, "set1", "set2", "dest_set")
			execute(&ctx, clientMaster, 4, randomSinterstoreThread)
			master_dest_interstore_set := clientMaster.SMembers(ctx, "dest_set")
			Expect(master_dest_interstore_set.Err()).NotTo(HaveOccurred())
			slave_dest_interstore_set := clientSlave.SMembers(ctx, "dest_set")
			Expect(slave_dest_interstore_set.Err()).NotTo(HaveOccurred())
			Expect(master_dest_interstore_set.Val()).To(Equal(slave_dest_interstore_set.Val()))
			clientMaster.Del(ctx, "set1", "set2", "dest_set")
			log.Println("randomSinterstore test success")
			//clientMaster.FlushAll(ctx)
			//time.Sleep(3 * time.Second)
			//go randomPfmergeThread(&ctx, clientMaster)
			//go randomPfmergeThread(&ctx, clientMaster)
			//go randomPfmergeThread(&ctx, clientMaster)
			//go randomPfmergeThread(&ctx, clientMaster)
			//time.Sleep(10 * time.Second)
			//master_hll_out := clientMaster.PFCount(ctx, "hll_out")
			//Expect(master_hll_out.Err()).NotTo(HaveOccurred())
			//slave_hll_out := clientSlave.PFCount(ctx, "hll_out")
			//Expect(slave_hll_out.Err()).NotTo(HaveOccurred())
			//Expect(master_hll_out.Val()).To(Equal(slave_hll_out.Val()))
			log.Println("randomZunionstore test start")
			clientMaster.Del(ctx, "zset1", "zset2", "zset_out")
			execute(&ctx, clientMaster, 4, randomZunionstoreThread)
			master_zset_out := clientMaster.ZRange(ctx, "zset_out", 0, -1)
			Expect(master_zset_out.Err()).NotTo(HaveOccurred())
			slave_zset_out := clientSlave.ZRange(ctx, "zset_out", 0, -1)
			Expect(slave_zset_out.Err()).NotTo(HaveOccurred())
			Expect(master_zset_out.Val()).To(Equal(slave_zset_out.Val()))
			clientMaster.Del(ctx, "zset1", "zset2", "zset_out")
			log.Println("randomZunionstore test success")

			log.Println("randomZinterstore test start")
			clientMaster.Del(ctx, "zset1", "zset2", "zset_out")
			execute(&ctx, clientMaster, 4, randomZinterstoreThread)
			master_dest_interstore_set = clientMaster.SMembers(ctx, "dest_set")
			Expect(master_dest_interstore_set.Err()).NotTo(HaveOccurred())
			slave_dest_interstore_set = clientSlave.SMembers(ctx, "dest_set")
			Expect(slave_dest_interstore_set.Err()).NotTo(HaveOccurred())
			Expect(master_dest_interstore_set.Val()).To(Equal(slave_dest_interstore_set.Val()))
			clientMaster.Del(ctx, "set1", "set2", "set_out")
			log.Println("randomZinterstore test success")

			log.Println("randomSunionstore test start")
			execute(&ctx, clientMaster, 4, randomSunionstroeThread)
			master_unionstore_set := clientMaster.SMembers(ctx, "set_out")
			Expect(master_unionstore_set.Err()).NotTo(HaveOccurred())
			slave_unionstore_set := clientSlave.SMembers(ctx, "set_out")
			Expect(slave_unionstore_set.Err()).NotTo(HaveOccurred())
			Expect(master_unionstore_set.Val()).To(Equal(slave_unionstore_set.Val()))
			clientMaster.Del(ctx, "set1", "set2", "set_out")
			log.Println("randomSunionstore test success")

			// Stream replication test
			log.Println("randomXadd test start")
			clientMaster.Del(ctx, "mystream")
			execute(&ctx, clientMaster, 4, randomXaddThread)
			masterStreamMessages := clientMaster.XRange(ctx, "mystream", "-", "+")
			slaveStreamMessages := clientSlave.XRange(ctx, "mystream", "-", "+")
			Expect(masterStreamMessages.Err()).NotTo(HaveOccurred())
			Expect(slaveStreamMessages.Err()).NotTo(HaveOccurred())
			Expect(masterStreamMessages.Val()).To(Equal(slaveStreamMessages.Val()))
			clientMaster.Del(ctx, "mystream")
			log.Println("randomXadd test success")

			// Blocked master-slave replication test
			log.Println("master-slave replication test start")
			lists := []string{"list0", "list1"}
			err := clientMaster.Del(ctx, lists...)
			Expect(err.Err()).NotTo(HaveOccurred())

			for i := 1; i <= 10; i++ {
				random_str1 := randomString(5)
				random_str2 := randomString(5)
				random_str3 := randomString(5)
				random_str4 := randomString(5)
				random_str5 := randomString(5)

				go issueBLPopCheck(&ctx, clientMaster, "list1", random_str1)
				time.Sleep(1 * time.Second)
				go issueBRpopCheck(&ctx, clientMaster, "list0", random_str2)
				time.Sleep(1 * time.Second)
				go issueBLPopCheck(&ctx, clientMaster, "list1", random_str3)
				time.Sleep(1 * time.Second)
				go issueBRpopCheck(&ctx, clientMaster, "list0", random_str4)
				time.Sleep(1 * time.Second)
				go issueBLPopCheck(&ctx, clientMaster, "list1", random_str5)
				time.Sleep(1 * time.Second)

				clientMaster.LPush(ctx, "list1", random_str1)
				time.Sleep(1 * time.Second)
				clientMaster.RPush(ctx, "list0", random_str2)
				time.Sleep(1 * time.Second)
				clientMaster.LPush(ctx, "list1", random_str3)
				time.Sleep(1 * time.Second)
				clientMaster.RPush(ctx, "list0", random_str4)
				time.Sleep(1 * time.Second)
				clientMaster.LPush(ctx, "list1", random_str5)

				for i := int64(0); i < clientMaster.LLen(ctx, "list0").Val(); i++ {
					Expect(clientMaster.LIndex(ctx, "list0", i)).To(Equal(clientSlave.LIndex(ctx, "list0", i)))
				}
				for i := int64(0); i < clientMaster.LLen(ctx, "list1").Val(); i++ {
					Expect(clientMaster.LIndex(ctx, "list1", i)).To(Equal(clientSlave.LIndex(ctx, "list1", i)))
				}
			}
			err = clientMaster.Del(ctx, lists...)

			// High frequency pop/push during unblocking process
			lists = []string{"blist0", "blist1"}
			err = clientMaster.Del(ctx, lists...)
			Expect(err.Err()).NotTo(HaveOccurred())

			for i := 1; i <= 5; i++ {
				go func() {
					clientMaster.BLPop(ctx, 0, lists...)
				}()
				go func() {
					clientMaster.BRPop(ctx, 0, lists...)
				}()
			}
			execute(&ctx, clientMaster, 5, issuePushPopFrequency)

			for i := int64(0); i < clientMaster.LLen(ctx, "blist0").Val(); i++ {
				Expect(clientMaster.LIndex(ctx, "blist0", i)).To(Equal(clientSlave.LIndex(ctx, "blist0", i)))
			}
			err = clientMaster.Del(ctx, lists...)
			log.Println("master-slave replication test success")
		})

	})

})
