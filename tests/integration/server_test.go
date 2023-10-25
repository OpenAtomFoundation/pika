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
		It("should  Auth", func() {
			r := client.Do(ctx, "AUTH", "foo")
			Expect(r.Err()).To(MatchError("ERR Client sent AUTH, but no password is set"))

			r = client.Do(ctx, "config", "set", "requirepass", "foobar")
			Expect(r.Val()).To(Equal("OK"))

			r = client.Do(ctx, "AUTH", "wrong!")
			Expect(r.Err()).To(MatchError("ERR invalid password"))

			r = client.Do(ctx, "AUTH", "foo", "bar")
			Expect(r.Err()).To(MatchError("ERR wrong number of arguments for 'auth' command"))

			r = client.Do(ctx, "AUTH", "foobar")
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
			Expect(err1).To(MatchError("ERR -NOPROTO unsupported protocol version"))

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

			r = client.Do(ctx, "hello", "3", "auth", "wrong")
			Expect(r.Err()).To(MatchError("ERR invalid password"))

			r = client.Do(ctx, "hello", "3", "auth", "foobar")
			Expect(r.Err()).NotTo(HaveOccurred())

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

		It("should BgRewriteAOF", func() {
			Skip("flaky test")

			val, err := client.BgRewriteAOF(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("Background append only file rewriting"))
		})

		// Test scenario: Execute the del command, after executing bgsave, the get data will be wrong
		It("should BgSave", func() {
			res := client.Set(ctx, "bgsave_key", "bgsave_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			_ = client.Set(ctx, "bgsave_key2", "bgsave_value3", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			_ = client.HSet(ctx, "bgsave_key3", "bgsave_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())

			res2, err2 := client.BgSave(ctx).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res2).To(ContainSubstring("Background saving started"))

			res = client.Set(ctx, "bgsave_key", "bgsave_value", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			res = client.Set(ctx, "bgsave_key2", "bgsave_value2", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			res = client.Set(ctx, "bgsave_key3", "bgsave_value3", 0)
			Expect(res.Err()).NotTo(HaveOccurred())
			hSet := client.HSet(ctx, "bgsave_key4", "bgsave_value4", 0)
			Expect(hSet.Err()).NotTo(HaveOccurred())

			_, err := client.Del(ctx, "bgsave_key").Result()
			Expect(err).NotTo(HaveOccurred())

			res2, err2 = client.BgSave(ctx).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res2).To(ContainSubstring("Background saving started"))

			val, err := client.Get(ctx, "bgsave_key2").Result()
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("bgsave_value2"))

			_, err = client.Del(ctx, "bgsave_key4").Result()
			Expect(err).NotTo(HaveOccurred())

			get := client.Get(ctx, "bgsave_key3")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("bgsave_value3"))
		})

		It("should ClientKill", func() {
			r := client.ClientKill(ctx, "1.1.1.1:1111")
			Expect(r.Err()).To(MatchError("ERR No such client"))
			Expect(r.Val()).To(Equal(""))
		})

		It("should client list", func() {
			Expect(client.ClientList(ctx).Val()).NotTo(BeEmpty())
			Expect(client.ClientList(ctx).Val()).To(ContainSubstring("addr="))
		})

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
			Expect(client.Set(ctx, "key", "value", 0).Val()).To(Equal("OK"))
			Expect(client.Do(ctx, "info", "keyspace", "1").Err()).NotTo(HaveOccurred())
			time.Sleep(1 * time.Second)
			size, err := client.DBSize(ctx).Result()
			time.Sleep(500 * time.Millisecond)
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			Expect(client.Del(ctx, "key").Val()).To(Equal(int64(1)))
			Expect(client.Do(ctx, "info", "keyspace", "1").Err()).NotTo(HaveOccurred())
			time.Sleep(1 * time.Second)
			size, err = client.DBSize(ctx).Result()
			time.Sleep(500 * time.Millisecond)
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

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

			Expect(client.Do(ctx, "info", "keyspace", "1").Err()).NotTo(HaveOccurred())
			time.Sleep(100 * time.Millisecond)
			Expect(client.Do(ctx, "dbsize").Val()).To(Equal(int64(4)))

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

		It("should Time", func() {
			tm, err := client.Time(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(tm).To(BeTemporally("~", time.Now(), 3*time.Second))
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

	Describe("Bit", func() {
		It("should BitCount", func() {
			set := client.Set(ctx, "key", "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount(ctx, "key", nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 0,
				End:   0,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 1,
				End:   1,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should BitOpAnd", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpAnd := client.BitOpAnd(ctx, "dest", "key1", "key2")
			Expect(bitOpAnd.Err()).NotTo(HaveOccurred())
			Expect(bitOpAnd.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should BitOpOr", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpOr := client.BitOpOr(ctx, "dest", "key1", "key2")
			Expect(bitOpOr.Err()).NotTo(HaveOccurred())
			Expect(bitOpOr.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("1"))
		})

		It("should BitOpXor", func() {
			set := client.Set(ctx, "key1", "\xff", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "\x0f", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpXor := client.BitOpXor(ctx, "dest", "key1", "key2")
			Expect(bitOpXor.Err()).NotTo(HaveOccurred())
			Expect(bitOpXor.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xf0"))
		})

		It("should BitOpNot", func() {
			set := client.Set(ctx, "key1", "\x00", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpNot := client.BitOpNot(ctx, "dest", "key1")
			Expect(bitOpNot.Err()).NotTo(HaveOccurred())
			Expect(bitOpNot.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xff"))
		})

		It("should BitPos", func() {
			err := client.Set(ctx, "mykey", "\xff\xf0\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPos(ctx, "mykey", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(12)))

			pos, err = client.BitPos(ctx, "mykey", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(0)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, -3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))
		})

		It("should BitPosSpan", func() {
			err := client.Set(ctx, "mykey", "\x00\xff\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should SetBit", func() {
			Expect(client.SetBit(ctx, "bit", 10086, 1).Val()).To(Equal(int64(0)))
			Expect(client.GetBit(ctx, "bit", 10086).Val()).To(Equal(int64(1)))
			Expect(client.GetBit(ctx, "bit", 100).Val()).To(Equal(int64(0)))
		})

		It("should GetBit", func() {
			setBit := client.SetBit(ctx, "key", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit(ctx, "key", 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit(ctx, "key", 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit(ctx, "key", 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
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
			Expect(client.PTTL(ctx, "key").Val()).NotTo(Equal(int64(-2)))

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
