package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Server", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(pikaOptions1())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("server", func() {
		It("should Auth", func() {
			cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Auth(ctx, "112121")
				pipe.Auth(ctx, "")
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR Client sent AUTH, but no password is set"))
			Expect(cmds[0].Err().Error()).To(ContainSubstring("ERR Client sent AUTH, but no password is set"))
			Expect(cmds[1].Err().Error()).To(ContainSubstring("ERR Client sent AUTH, but no password is set"))

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(1)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
			Expect(stats.TotalConns).To(Equal(uint32(1)))
			Expect(stats.IdleConns).To(Equal(uint32(1)))
		})

		//It("should hello", func() {
		//	cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		//		pipe.Hello(ctx, 2, "", "", "")
		//		return nil
		//	})
		//	Expect(err).NotTo(HaveOccurred())
		//	m, err := cmds[0].(*redis.MapStringInterfaceCmd).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(m["proto"]).To(Equal(int64(2)))
		//})

		It("should Echo", func() {
			pipe := client.Pipeline()
			echo := pipe.Echo(ctx, "hello")
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(echo.Err()).NotTo(HaveOccurred())
			Expect(echo.Val()).To(Equal("hello"))
		})

		It("should Ping", func() {
			ping := client.Ping(ctx)
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
		})

		//It("should Wait", func() {
		//	const wait = 3 * time.Second
		//
		//	// assume testing on single redis instance
		//	start := time.Now()
		//	val, err := client.Wait(ctx, 1, wait).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal(int64(0)))
		//	Expect(time.Now()).To(BeTemporally("~", start.Add(wait), 3*time.Second))
		//})

		It("should Select", func() {
			pipe := client.Pipeline()
			sel := pipe.Select(ctx, 0)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(sel.Err()).NotTo(HaveOccurred())
			Expect(sel.Val()).To(Equal("OK"))
		})

		//It("should SwapDB", func() {
		//	pipe := client.Pipeline()
		//	sel := pipe.SwapDB(ctx, 1, 2)
		//	_, err := pipe.Exec(ctx)
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	Expect(sel.Err()).NotTo(HaveOccurred())
		//	Expect(sel.Val()).To(Equal("OK"))
		//})

		It("should BgRewriteAOF", func() {
			Skip("flaky test")

			val, err := client.BgRewriteAOF(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("Background append only file rewriting"))
		})

		// Test scenario: Execute the del command, after executing bgsave, the get data will be wrong
		It("should BgSave", func() {
			res := client.Set(ctx, "bgsava_key", "bgsava_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			_ = client.Set(ctx, "bgsava_key2", "bgsava_value3", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			_ = client.HSet(ctx, "bgsava_key3", "bgsava_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())

			res2, err2 := client.BgSave(ctx).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res2).To(ContainSubstring("Background saving started"))

			res = client.Set(ctx, "bgsava_key", "bgsava_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			res = client.Set(ctx, "bgsava_key2", "bgsava_value2", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			res = client.Set(ctx, "bgsava_key3", "bgsava_value3", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			hSet := client.HSet(ctx, "bgsava_key4", "bgsava_value4", 0)
			Expect(hSet.Err()).NotTo(HaveOccurred())

			_, err := client.Del(ctx, "bgsava_key").Result()
			Expect(err).NotTo(HaveOccurred())

			res2, err2 = client.BgSave(ctx).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res2).To(ContainSubstring("Background saving started"))

			val, err := client.Get(ctx, "bgsava_key2").Result()
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("bgsava_value2"))

			_, err = client.Del(ctx, "bgsava_key4").Result()
			Expect(err).NotTo(HaveOccurred())

			get := client.Get(ctx, "bgsava_key3")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("bgsava_value3"))
		})

		//It("Should CommandGetKeys", func() {
		//	keys, err := client.CommandGetKeys(ctx, "MSET", "a", "b", "c", "d", "e", "f").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(keys).To(Equal([]string{"a", "c", "e"}))
		//
		//	keys, err = client.CommandGetKeys(ctx, "EVAL", "not consulted", "3", "key1", "key2", "key3", "arg1", "arg2", "arg3", "argN").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(keys).To(Equal([]string{"key1", "key2", "key3"}))
		//
		//	keys, err = client.CommandGetKeys(ctx, "SORT", "mylist", "ALPHA", "STORE", "outlist").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(keys).To(Equal([]string{"mylist", "outlist"}))
		//
		//	_, err = client.CommandGetKeys(ctx, "FAKECOMMAND", "arg1", "arg2").Result()
		//	Expect(err).To(HaveOccurred())
		//	Expect(err).To(MatchError("ERR Invalid command specified"))
		//})

		//It("should CommandGetKeysAndFlags", func() {
		//	keysAndFlags, err := client.CommandGetKeysAndFlags(ctx, "LMOVE", "mylist1", "mylist2", "left", "left").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(keysAndFlags).To(Equal([]redis.KeyFlags{
		//		{
		//			Key:   "mylist1",
		//			Flags: []string{"RW", "access", "delete"},
		//		},
		//		{
		//			Key:   "mylist2",
		//			Flags: []string{"RW", "insert"},
		//		},
		//	}))
		//
		//	_, err = client.CommandGetKeysAndFlags(ctx, "FAKECOMMAND", "arg1", "arg2").Result()
		//	Expect(err).To(HaveOccurred())
		//	Expect(err).To(MatchError("ERR Invalid command specified"))
		//})

		// todo 存在bug，待修复
		//It("should ClientKill", func() {
		//	r := client.ClientKill(ctx, "1.1.1.1:1111")
		//	Expect(r.Err()).To(MatchError("ERR No such client"))
		//	Expect(r.Val()).To(Equal(""))
		//})

		//It("should ClientKillByFilter", func() {
		//	r := client.ClientKillByFilter(ctx, "TYPE", "test")
		//	Expect(r.Err()).To(MatchError("ERR Unknown client type 'test'"))
		//	Expect(r.Val()).To(Equal(int64(0)))
		//})

		//It("should ClientID", func() {
		//	err := client.ClientID(ctx).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(client.ClientID(ctx).Val()).To(BeNumerically(">=", 0))
		//})
		//
		//It("should ClientUnblock", func() {
		//	id := client.ClientID(ctx).Val()
		//	r, err := client.ClientUnblock(ctx, id).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(r).To(Equal(int64(0)))
		//})
		//
		//It("should ClientUnblockWithError", func() {
		//	id := client.ClientID(ctx).Val()
		//	r, err := client.ClientUnblockWithError(ctx, id).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(r).To(Equal(int64(0)))
		//})

		//It("should ClientInfo", func() {
		//	info, err := client.ClientInfo(ctx).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(info).NotTo(BeNil())
		//})

		//It("should ClientPause", func() {
		//	err := client.ClientPause(ctx, time.Second).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	start := time.Now()
		//	err = client.Ping(ctx).Err()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(time.Now()).To(BeTemporally("~", start.Add(time.Second), 800*time.Millisecond))
		//})

		It("should ClientSetName and ClientGetName", func() {
			pipe := client.Pipeline()
			set := pipe.ClientSetName(ctx, "theclientname")
			get := pipe.ClientGetName(ctx)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(BeTrue())

			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("theclientname"))
		})

		It("should ConfigGet", func() {
			val, err := client.ConfigGet(ctx, "*").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).NotTo(BeEmpty())
		})

		It("should ConfigResetStat", func() {
			r := client.ConfigResetStat(ctx)
			Expect(r.Err()).NotTo(HaveOccurred())
			Expect(r.Val()).To(Equal("OK"))
		})

		It("should ConfigSet", func() {
			configGet := client.ConfigGet(ctx, "maxmemory")
			Expect(configGet.Err()).NotTo(HaveOccurred())
			Expect(configGet.Val()).To(HaveLen(1))
			_, ok := configGet.Val()["maxmemory"]
			Expect(ok).To(BeTrue())

			//configSet := client.ConfigSet(ctx, "maxmemory", configGet.Val()["maxmemory"])
			//Expect(configSet.Err()).NotTo(HaveOccurred())
			//Expect(configSet.Val()).To(Equal("OK"))
		})

		It("should ConfigRewrite", func() {
			configRewrite := client.ConfigRewrite(ctx)
			Expect(configRewrite.Err()).NotTo(HaveOccurred())
			Expect(configRewrite.Val()).To(Equal("OK"))
		})

		It("should DBSize", func() {
			size, err := client.DBSize(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

		It("should Info", func() {
			info := client.Info(ctx)
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
		})

		It("should Info cpu", func() {
			info := client.Info(ctx, "cpu")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
		})

		//It("should Info cpu and memory", func() {
		//	info := client.Info(ctx, "cpu", "memory")
		//	Expect(info.Err()).NotTo(HaveOccurred())
		//	Expect(info.Val()).NotTo(Equal(""))
		//	Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
		//	Expect(info.Val()).To(ContainSubstring(`memory`))
		//})
		//
		//It("should LastSave", func() {
		//	lastSave := client.LastSave(ctx)
		//	Expect(lastSave.Err()).NotTo(HaveOccurred())
		//	Expect(lastSave.Val()).NotTo(Equal(0))
		//})

		//It("should Save", func() {
		//
		//	val := client.Save(ctx)
		//	fmt.Println(val)
		//
		//	// workaround for "ERR Background save already in progress"
		//	Eventually(func() string {
		//		return client.Save(ctx).Val()
		//	}, "10s").Should(Equal("OK"))
		//})

		// todo 待回滚
		//It("should SlaveOf", func() {
		//	slaveOf := client.SlaveOf(ctx, "localhost", "8888")
		//	Expect(slaveOf.Err()).NotTo(HaveOccurred())
		//	Expect(slaveOf.Val()).To(Equal("OK"))
		//
		//	slaveOf = client.SlaveOf(ctx, "NO", "ONE")
		//	Expect(slaveOf.Err()).NotTo(HaveOccurred())
		//	Expect(slaveOf.Val()).To(Equal("OK"))
		//})

		It("should Time", func() {
			tm, err := client.Time(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(tm).To(BeTemporally("~", time.Now(), 3*time.Second))
		})

		//It("should Command", func() {
		//	cmds, err := client.Command(ctx).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(len(cmds)).To(BeNumerically("~", 240, 25))
		//
		//	cmd := cmds["mget"]
		//	Expect(cmd.Name).To(Equal("mget"))
		//	Expect(cmd.Arity).To(Equal(int8(-2)))
		//	Expect(cmd.Flags).To(ContainElement("readonly"))
		//	Expect(cmd.FirstKeyPos).To(Equal(int8(1)))
		//	Expect(cmd.LastKeyPos).To(Equal(int8(-1)))
		//	Expect(cmd.StepCount).To(Equal(int8(1)))
		//
		//	cmd = cmds["ping"]
		//	Expect(cmd.Name).To(Equal("ping"))
		//	Expect(cmd.Arity).To(Equal(int8(-1)))
		//	Expect(cmd.Flags).To(ContainElement("fast"))
		//	Expect(cmd.FirstKeyPos).To(Equal(int8(0)))
		//	Expect(cmd.LastKeyPos).To(Equal(int8(0)))
		//	Expect(cmd.StepCount).To(Equal(int8(0)))
		//})
		//
		//It("should return all command names", func() {
		//	cmdList := client.CommandList(ctx, nil)
		//	Expect(cmdList.Err()).NotTo(HaveOccurred())
		//	cmdNames := cmdList.Val()
		//
		//	Expect(cmdNames).NotTo(BeEmpty())
		//
		//	// Assert that some expected commands are present in the list
		//	Expect(cmdNames).To(ContainElement("get"))
		//	Expect(cmdNames).To(ContainElement("set"))
		//	Expect(cmdNames).To(ContainElement("hset"))
		//})
		//
		//It("should filter commands by module", func() {
		//	filter := &redis.FilterBy{
		//		Module: "JSON",
		//	}
		//	cmdList := client.CommandList(ctx, filter)
		//	Expect(cmdList.Err()).NotTo(HaveOccurred())
		//	Expect(cmdList.Val()).To(HaveLen(0))
		//})
		//
		//It("should filter commands by ACL category", func() {
		//
		//	filter := &redis.FilterBy{
		//		ACLCat: "admin",
		//	}
		//
		//	cmdList := client.CommandList(ctx, filter)
		//	Expect(cmdList.Err()).NotTo(HaveOccurred())
		//	cmdNames := cmdList.Val()
		//
		//	// Assert that the returned list only contains commands from the admin ACL category
		//	Expect(len(cmdNames)).To(BeNumerically(">", 10))
		//})
		//
		//It("should filter commands by pattern", func() {
		//	filter := &redis.FilterBy{
		//		Pattern: "*GET*",
		//	}
		//	cmdList := client.CommandList(ctx, filter)
		//	Expect(cmdList.Err()).NotTo(HaveOccurred())
		//	cmdNames := cmdList.Val()
		//
		//	// Assert that the returned list only contains commands that match the given pattern
		//	Expect(cmdNames).To(ContainElement("get"))
		//	Expect(cmdNames).To(ContainElement("getbit"))
		//	Expect(cmdNames).To(ContainElement("getrange"))
		//	Expect(cmdNames).NotTo(ContainElement("set"))
		//})
	})
})
