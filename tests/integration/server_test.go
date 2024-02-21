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
		It("should  Auth2", func() {
			r := client.Do(ctx, "AUTH", "foo")
			Expect(r.Err()).To(MatchError("ERR Client sent AUTH, but no password is set"))

			r = client.Do(ctx, "config", "set", "requirepass", "foobar")
			Expect(r.Val()).To(Equal("OK"))

			r = client.Do(ctx, "AUTH", "wrong!")
			Expect(r.Err()).To(MatchError("WRONGPASS invalid username-password pair or user is disabled."))

			// r = client.Do(ctx, "AUTH", "foo", "bar")
			// Expect(r.Err()).To(MatchError("ERR wrong number of arguments for 'auth' command"))

			r = client.Do(ctx, "AUTH", "default", "foobar")
			Expect(r.Val()).To(Equal("OK"))

			r = client.Do(ctx, "config", "set", "requirepass", "")
			Expect(r.Val()).To(Equal("OK"))

		})

		It("should hello", func() {
			cmds, _ := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Hello(ctx, 1, "", "", "")
				pipe.Hello(ctx, 2, "", "", "")
				pipe.Hello(ctx, 3, "", "", "")
				return nil
			})
			_, err1 := cmds[0].(*redis.MapStringInterfaceCmd).Result()
			m2, err2 := cmds[1].(*redis.MapStringInterfaceCmd).Result()
			m3, err3 := cmds[2].(*redis.MapStringInterfaceCmd).Result()
			Expect(err1).To(MatchError("NOPROTO unsupported protocol version"))

			Expect(err2).NotTo(HaveOccurred())
			Expect(m2["proto"]).To(Equal(int64(2)))

			Expect(err3).NotTo(HaveOccurred())
			Expect(m3["proto"]).To(Equal(int64(2)))

			r := client.Do(ctx, "hello", "auth")
			Expect(r.Err()).To(MatchError("ERR Protocol version is not an integer or out of range"))

			r = client.Do(ctx, "hello", "3", "SETNAME", "pika")
			Expect(r.Err()).NotTo(HaveOccurred())

			r = client.Do(ctx, "CLIENT", "GETNAME")
			Expect(r.Val()).To(Equal("pika"))

			r = client.Do(ctx, "config", "set", "requirepass", "foobar")
			Expect(r.Val()).To(Equal("OK"))

			// r = client.Do(ctx, "hello", "3", "auth", "wrong")
			// Expect(r.Err()).To(MatchError("ERR invalid password"))
			//
			// r = client.Do(ctx, "hello", "3", "auth", "foobar")
			// Expect(r.Err()).NotTo(HaveOccurred())

			r = client.Do(ctx, "config", "set", "requirepass", "")
		})

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

			sel = pipe.Select(ctx, 4)
			_, err = pipe.Exec(ctx)
			Expect(err).To(MatchError("ERR invalid DB index for 'select DB index is out of range'"))
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
		//It("should BgSave", func() {
		//	res := client.Set(ctx, "bgsava_key", "bgsava_value", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	_ = client.Set(ctx, "bgsava_key2", "bgsava_value3", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	_ = client.HSet(ctx, "bgsava_key3", "bgsava_value", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//
		//	res2, err2 := client.BgSave(ctx).Result()
		//	Expect(err2).NotTo(HaveOccurred())
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	Expect(res2).To(ContainSubstring("Background saving started"))
		//
		//	res = client.Set(ctx, "bgsava_key", "bgsava_value", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	res = client.Set(ctx, "bgsava_key2", "bgsava_value2", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	res = client.Set(ctx, "bgsava_key3", "bgsava_value3", 0)
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	hSet := client.HSet(ctx, "bgsava_key4", "bgsava_value4", 0)
		//	Expect(hSet.Err()).NotTo(HaveOccurred())
		//
		//	_, err := client.Del(ctx, "bgsava_key").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	res2, err2 = client.BgSave(ctx).Result()
		//	Expect(err2).NotTo(HaveOccurred())
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	Expect(res2).To(ContainSubstring("Background saving started"))
		//
		//	val, err := client.Get(ctx, "bgsava_key2").Result()
		//	Expect(res.Err()).NotTo(HaveOccurred())
		//	Expect(val).To(ContainSubstring("bgsava_value2"))
		//
		//	_, err = client.Del(ctx, "bgsava_key4").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	get := client.Get(ctx, "bgsava_key3")
		//	Expect(get.Err()).NotTo(HaveOccurred())
		//	Expect(get.Val()).To(Equal("bgsava_value3"))
		//})

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

		It("should client list", func() {
			Expect(client.ClientList(ctx).Val()).NotTo(BeEmpty())
			Expect(client.ClientList(ctx).Val()).To(ContainSubstring("addr="))
		})

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

		//It("should DBSize", func() {
		//	Expect(client.Set(ctx, "key", "value", 0).Val()).To(Equal("OK"))
		//	Expect(client.Do(ctx, "info", "keyspace", "1").Err()).NotTo(HaveOccurred())
		//	time.Sleep(2 * time.Second)
		//	size, err := client.DBSize(ctx).Result()
		//	time.Sleep(1 * time.Second)
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(size).To(Equal(int64(1)))
		//
		//	Expect(client.Del(ctx, "key").Val()).To(Equal(int64(1)))
		//	Expect(client.Do(ctx, "info", "keyspace", "1").Err()).NotTo(HaveOccurred())
		//	time.Sleep(2 * time.Second)
		//	size, err = client.DBSize(ctx).Result()
		//	time.Sleep(1 * time.Second)
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(size).To(Equal(int64(0)))
		//})

		It("should Info", func() {
			info := client.Info(ctx)
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))

			info = client.Info(ctx, "replication")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring("ReplicationID"))
			Expect(info.Val()).To(ContainSubstring("role"))
			Expect(info.Val()).To(ContainSubstring("connected_slaves"))

			info = client.Info(ctx, "cpu")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
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
		It("should LastSave", func() {
			lastSave := client.LastSave(ctx)
			Expect(lastSave.Err()).NotTo(HaveOccurred())
			//Expect(lastSave.Val()).To(Equal(int64(0)))

			bgSaveTime1 := time.Now().Unix()
			bgSave, err := client.BgSave(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(bgSave).To(ContainSubstring("Background saving started"))
			time.Sleep(1 * time.Second)
			bgSaveTime2 := time.Now().Unix()

			lastSave = client.LastSave(ctx)
			Expect(lastSave.Err()).NotTo(HaveOccurred())
			Expect(lastSave.Val()).To(BeNumerically(">=", bgSaveTime1))
			Expect(lastSave.Val()).To(BeNumerically("<=", bgSaveTime2))

		})

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

		// fix: https://github.com/OpenAtomFoundation/pika/issues/2168
		It("should SlaveOf itself", func() {
			slaveOf := client.SlaveOf(ctx, "127.0.0.1", "9221")
			Expect(slaveOf.Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

			slaveOf = client.SlaveOf(ctx, "localhost", "9221")
			Expect(slaveOf.Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))

			slaveOf = client.SlaveOf(ctx, "loCalHoSt", "9221")
			Expect(slaveOf.Err()).To(MatchError("ERR The master ip:port and the slave ip:port are the same"))
		})

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

		It("should keyspace", func() {
			r := client.Set(ctx, "key", "value", 0)
			Expect(r.Err()).NotTo(HaveOccurred())
			Expect(r.Val()).To(Equal("OK"))
			Expect(client.Get(ctx, "key").Val()).To(Equal("value"))
			Expect(client.Del(ctx, "key").Val()).To(Equal(int64(1)))
			Expect(client.Get(ctx, "key").Err()).To(MatchError(redis.Nil))

			Expect(client.Set(ctx, "key1", "value1", 0).Val()).To(Equal("OK"))
			Expect(client.Set(ctx, "key2", "value2", 0).Val()).To(Equal("OK"))
			Expect(client.Set(ctx, "key3", "value3", 0).Val()).To(Equal("OK"))
			Expect(client.Del(ctx, "key1", "key2", "key3").Val()).To(Equal(int64(3)))

			Expect(client.Set(ctx, "key_1", "value1", 0).Val()).To(Equal("OK"))
			Expect(client.Set(ctx, "key_2", "value2", 0).Val()).To(Equal("OK"))
			Expect(client.Keys(ctx, "key_*").Val()).To(Equal([]string{"key_1", "key_2"}))

			Expect(client.Set(ctx, "key1", "value1", 0).Val()).To(Equal("OK"))
			Expect(client.Set(ctx, "key2", "value2", 0).Val()).To(Equal("OK"))
			Expect(client.Keys(ctx, "*").Err()).NotTo(HaveOccurred())

			Expect(client.Del(ctx, "key_1", "key_2", "key1", "key2").Val()).To(Equal(int64(4)))

			Expect(client.Set(ctx, "NewKey", "value", 0).Val()).To(Equal("OK"))
			Expect(client.Exists(ctx, "NewKey").Val()).To(Equal(int64(1)))
			Expect(client.Del(ctx, "NewKey").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "NewKey").Val()).To(Equal(int64(0)))

			Expect(client.Do(ctx, "foocommand").Err()).To(MatchError("ERR unknown command \"foocommand\""))

		})

		It("should Incr", func() {
			Expect(client.Incr(ctx, "nonexist").Val()).To(Equal(int64(1)))
			Expect(client.Get(ctx, "nonexist").Val()).To(Equal("1"))

			Expect(client.Incr(ctx, "nonexist").Val()).To(Equal(int64(2)))

			Expect(client.Set(ctx, "existval", 10, 0).Val()).To(Equal("OK"))
			Expect(client.Incr(ctx, "existval").Val()).To(Equal(int64(11)))

			Expect(client.IncrByFloat(ctx, "nonexist2", 1.0).Val()).To(Equal(float64(1)))
			Expect(client.Get(ctx, "nonexist2").Val()).To(Equal("1"))
			Expect(client.IncrByFloat(ctx, "nonexist2", 0.99).Val()).To(Equal(1.99))

		})

		It("should Incr big num", func() {
			Expect(client.Set(ctx, "existval", 17179869184, 0).Val()).To(Equal("OK"))
			Expect(client.Incr(ctx, "existval").Val()).To(Equal(int64(17179869185)))

			Expect(client.Set(ctx, "existval2", 17179869184, 0).Val()).To(Equal("OK"))
			Expect(client.IncrBy(ctx, "existval2", 17179869184).Val()).To(Equal(int64(34359738368)))
		})

		It("should unlink", func() {
			command := "UNLINK"
			Expect(client.MSet(ctx, "key1", "value1", "key2", "value2").Val()).To(Equal("OK"))
			Expect(client.Do(ctx, command, "key1", "key2").Val()).To(Equal(int64(2)))
			Expect(client.Exists(ctx, "key", "key2").Val()).To(Equal(int64(0)))

			Expect(client.LPush(ctx, "mlist", "value1", "value2").Val()).To(Equal(int64(2)))
			Expect(client.Do(ctx, command, "mlist").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "mlist").Val()).To(Equal(int64(0)))

			Expect(client.SAdd(ctx, "mset", "value1", "value2").Val()).To(Equal(int64(2)))
			Expect(client.Do(ctx, command, "mset").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "mset").Val()).To(Equal(int64(0)))

			Expect(client.HSet(ctx, "mhash", "key1", "value1").Val()).To(Equal(int64(1)))
			Expect(client.HSet(ctx, "mhash", "key2", "value2").Val()).To(Equal(int64(1)))
			Expect(client.Do(ctx, command, "mhash").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "mhash").Val()).To(Equal(int64(0)))
		})

		It("should type", func() {
			Expect(client.Set(ctx, "key", "value", 0).Val()).To(Equal("OK"))
			Expect(client.LPush(ctx, "mlist", "hello").Val()).To(Equal(int64(1)))
			Expect(client.SAdd(ctx, "mset", "world").Val()).To(Equal(int64(1)))

			Expect(client.Type(ctx, "key").Val()).To(Equal("string"))
			Expect(client.Type(ctx, "mlist").Val()).To(Equal("list"))
			Expect(client.Type(ctx, "mset").Val()).To(Equal("set"))
		})
	})

	Describe("Expire", func() {
		It("should expire", func() {
			Expect(client.Set(ctx, "key_3s", "value", 0).Val()).To(Equal("OK"))
			Expect(client.Expire(ctx, "key_3s", 3*time.Second).Val()).To(Equal(true))
			Expect(client.TTL(ctx, "key").Val()).NotTo(Equal(int64(-2)))

			time.Sleep(4 * time.Second)
			Expect(client.TTL(ctx, "key_3s").Val()).To(Equal(time.Duration(-2)))
			Expect(client.Get(ctx, "key_3s").Err()).To(MatchError(redis.Nil))
			Expect(client.Exists(ctx, "key_3s").Val()).To(Equal(int64(0)))

			Expect(client.Do(ctx, "expire", "foo", "bar").Err()).To(MatchError("ERR value is not an integer or out of range"))

		})

		It("should pexpire", func() {
			Expect(client.Set(ctx, "key_3000ms", "value", 0).Val()).To(Equal("OK"))
			Expect(client.PExpire(ctx, "key_3000ms", 3000*time.Millisecond).Val()).To(Equal(true))
			Expect(client.PTTL(ctx, "key_3000ms").Val()).NotTo(Equal(time.Duration(-2)))

			time.Sleep(4 * time.Second)
			Expect(client.PTTL(ctx, "key_3000ms").Val()).To(Equal(time.Duration(-2)))
			Expect(client.Get(ctx, "key_3000ms").Err()).To(MatchError(redis.Nil))
			Expect(client.Exists(ctx, "key_3000ms").Val()).To(Equal(int64(0)))

			Expect(client.Do(ctx, "pexpire", "key_3000ms", "err").Err()).To(MatchError("ERR value is not an integer or out of range"))
		})

		It("should expireat", func() {
			Expect(client.Set(ctx, "foo", "bar", 0).Val()).To(Equal("OK"))
			Expect(client.Do(ctx, "expireat", "foo", "1293840000").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "foo").Val()).To(Equal(int64(0)))
		})

		It("should pexpirat", func() {
			Expect(client.Set(ctx, "foo", "bar", 0).Val()).To(Equal("OK"))
			Expect(client.Do(ctx, "pexpireat", "foo", "1293840000").Val()).To(Equal(int64(1)))
			Expect(client.Exists(ctx, "foo").Val()).To(Equal(int64(0)))

		})
	})
})
