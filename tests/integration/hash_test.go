package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("List Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(pikarOptions1())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("hashes", func() {
		It("should HDel", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hDel := client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(1)))

			hDel = client.HDel(ctx, "hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(0)))
		})

		It("should HExists", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hExists := client.HExists(ctx, "hash", "key")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(true))

			hExists = client.HExists(ctx, "hash", "key1")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(false))
		})

		It("should HGet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			hGet = client.HGet(ctx, "hash", "key1")
			Expect(hGet.Err()).To(Equal(redis.Nil))
			Expect(hGet.Val()).To(Equal(""))
		})

		It("should HGetAll", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			m, err := client.HGetAll(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("should scan", func() {
			now := time.Now()

			err := client.HMSet(ctx, "hash", "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.HGetAll(ctx, "hash")
			Expect(res.Err()).NotTo(HaveOccurred())

			type data struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time TimeValue `redis:"time"`
			}
			var d data
			Expect(res.Scan(&d)).NotTo(HaveOccurred())
			Expect(d.Time.UnixNano()).To(Equal(now.UnixNano()))
			d.Time.Time = time.Time{}
			Expect(d).To(Equal(data{
				Key1: "hello1",
				Key2: 123,
				Time: TimeValue{Time: time.Time{}},
			}))

			//type data2 struct {
			//	Key1 string    `redis:"key1"`
			//	Key2 int       `redis:"key2"`
			//	Time time.Time `redis:"time"`
			//}
			////err = client.HSet(ctx, "hash", &data2{
			////	Key1: "hello2",
			////	Key2: 200,
			////	Time: now,
			////}).Err()
			////Expect(err).NotTo(HaveOccurred())
			//
			//var d2 data2
			//err = client.HMGet(ctx, "hash", "key1", "key2", "time").Scan(&d2)
			//Expect(err).NotTo(HaveOccurred())
			//Expect(d2.Key1).To(Equal("hello2"))
			//Expect(d2.Key2).To(Equal(200))
			//Expect(d2.Time.Unix()).To(Equal(now.Unix()))
		})

		It("should HIncrBy", func() {
			hSet := client.HSet(ctx, "hash", "key", "5")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hIncrBy := client.HIncrBy(ctx, "hash", "key", 1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(6)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(5)))

			hIncrBy = client.HIncrBy(ctx, "hash", "key", -10)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(-5)))
		})

		It("should HIncrByFloat", func() {
			hSet := client.HSet(ctx, "hash", "field", "10.50")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hIncrByFloat := client.HIncrByFloat(ctx, "hash", "field", 0.1)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(10.6))

			hSet = client.HSet(ctx, "hash", "field", "5.0e3")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(0)))

			hIncrByFloat = client.HIncrByFloat(ctx, "hash", "field", 2.0e2)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should HKeys", func() {
			hkeys := client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{}))

			hset := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hset.Err()).NotTo(HaveOccurred())
			hset = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hset.Err()).NotTo(HaveOccurred())

			hkeys = client.HKeys(ctx, "hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{"key1", "key2"}))
		})

		It("should HLen", func() {
			hSet := client.HSet(ctx, "hash", "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			hSet = client.HSet(ctx, "hash", "key2", "hello2")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hLen := client.HLen(ctx, "hash")
			Expect(hLen.Err()).NotTo(HaveOccurred())
			Expect(hLen.Val()).To(Equal(int64(2)))
		})

		It("should HMGet", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.HMGet(ctx, "hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"hello1"}))
		})

		It("should HSet", func() {
			_, err := client.Del(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())

			ok, err := client.HSet(ctx, "hash", map[string]interface{}{
				"key1": "hello1",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			ok, err = client.HSet(ctx, "hash", map[string]interface{}{
				"key2": "hello2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(int64(1)))

			v, err := client.HGet(ctx, "hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello1"))

			v, err = client.HGet(ctx, "hash", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello2"))

			keys, err := client.HKeys(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf([]string{"key1", "key2"}))
		})

		It("should HSet", func() {
			hSet := client.HSet(ctx, "hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(int64(1)))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
				Set6 string                 `redis:"set6,omitempty"`
			}

			// 命令格式不对：hset hash set1 val1 set2 1024 set3 2000000 set4
			//hSet = client.HSet(ctx, "hash", &set{
			//	Set1: "val1",
			//	Set2: 1024,
			//	Set3: 2 * time.Millisecond,
			//	Set4: nil,
			//	Set5: map[string]interface{}{"k1": 1},
			//})
			//Expect(hSet.Err()).NotTo(HaveOccurred())
			//Expect(hSet.Val()).To(Equal(int64(4)))

			//hMGet := client.HMGet(ctx, "hash", "set1", "set2", "set3", "set4", "set5", "set6")
			//Expect(hMGet.Err()).NotTo(HaveOccurred())
			//Expect(hMGet.Val()).To(Equal([]interface{}{
			//	"val1",
			//	"1024",
			//	strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
			//	"",
			//	nil,
			//	nil,
			//}))

			//hSet = client.HSet(ctx, "hash2", &set{
			//	Set1: "val2",
			//	Set6: "val",
			//})
			//Expect(hSet.Err()).NotTo(HaveOccurred())
			//Expect(hSet.Val()).To(Equal(int64(5)))
			//
			//hMGet = client.HMGet(ctx, "hash2", "set1", "set6")
			//Expect(hMGet.Err()).NotTo(HaveOccurred())
			//Expect(hMGet.Val()).To(Equal([]interface{}{
			//	"val2",
			//	"val",
			//}))
		})

		It("should HSetNX", func() {
			res := client.Del(ctx, "hash")
			Expect(res.Err()).NotTo(HaveOccurred())

			hSetNX := client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(true))

			hSetNX = client.HSetNX(ctx, "hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(false))

			hGet := client.HGet(ctx, "hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))
		})

		It("should HVals", func() {
			err := client.HSet(ctx, "hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet(ctx, "hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.HVals(ctx, "hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"hello1", "hello2"}))

			var slice []string
			err = client.HVals(ctx, "hash").ScanSlice(&slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice).To(Equal([]string{"hello1", "hello2"}))
		})

		//It("should HRandField", func() {
		//	err := client.HSet(ctx, "hash", "key1", "hello1").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//	err = client.HSet(ctx, "hash", "key2", "hello2").Err()
		//	Expect(err).NotTo(HaveOccurred())
		//
		//	//v := client.HRandField(ctx, "hash", 1)
		//	//Expect(v.Err()).NotTo(HaveOccurred())
		//	//Expect(v.Val()).To(Or(Equal([]string{"key1"}), Equal([]string{"key2"})))
		//
		//	v := client.HRandField(ctx, "hash", 0)
		//	Expect(v.Err()).NotTo(HaveOccurred())
		//	Expect(v.Val()).To(HaveLen(0))
		//
		//	kv, err := client.HRandFieldWithValues(ctx, "hash", 1).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(kv).To(Or(
		//		Equal([]redis.KeyValue{{Key: "key1", Value: "hello1"}}),
		//		Equal([]redis.KeyValue{{Key: "key2", Value: "hello2"}}),
		//	))
		//})
	})
})
