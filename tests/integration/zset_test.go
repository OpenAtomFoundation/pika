package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Zset Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		if GlobalBefore != nil {
			GlobalBefore(ctx, client)
		}
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	//It("should BZPopMax", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  2,
	//		Member: "two",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  3,
	//		Member: "three",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	member, err := client.BZPopMax(ctx, 0, "zset1", "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(member).To(Equal(&redis.ZWithKey{
	//		Z: redis.Z{
	//			Score:  3,
	//			Member: "three",
	//		},
	//		Key: "zset1",
	//	}))
	//})

	//It("should BZPopMax blocks", func() {
	//	started := make(chan bool)
	//	done := make(chan bool)
	//	go func() {
	//		defer GinkgoRecover()
	//
	//		started <- true
	//		bZPopMax := client.BZPopMax(ctx, 0, "zset")
	//		Expect(bZPopMax.Err()).NotTo(HaveOccurred())
	//		Expect(bZPopMax.Val()).To(Equal(&redis.ZWithKey{
	//			Z: redis.Z{
	//				Member: "a",
	//				Score:  1,
	//			},
	//			Key: "zset",
	//		}))
	//		done <- true
	//	}()
	//	<-started
	//
	//	select {
	//	case <-done:
	//		Fail("BZPopMax is not blocked")
	//	case <-time.After(time.Second):
	//		// ok
	//	}
	//
	//	zAdd := client.ZAdd(ctx, "zset", redis.Z{
	//		Member: "a",
	//		Score:  1,
	//	})
	//	Expect(zAdd.Err()).NotTo(HaveOccurred())
	//
	//	select {
	//	case <-done:
	//		// ok
	//	case <-time.After(time.Second):
	//		Fail("BZPopMax is still blocked")
	//	}
	//})

	//It("should BZPopMax timeout", func() {
	//	val, err := client.BZPopMax(ctx, time.Second, "zset1").Result()
	//	Expect(err).To(Equal(redis.Nil))
	//	Expect(val).To(BeNil())
	//
	//	Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	//
	//	stats := client.PoolStats()
	//	Expect(stats.Hits).To(Equal(uint32(2)))
	//	Expect(stats.Misses).To(Equal(uint32(1)))
	//	Expect(stats.Timeouts).To(Equal(uint32(0)))
	//})

	//It("should BZPopMin", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  2,
	//		Member: "two",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{
	//		Score:  3,
	//		Member: "three",
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	member, err := client.BZPopMin(ctx, 0, "zset1", "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(member).To(Equal(&redis.ZWithKey{
	//		Z: redis.Z{
	//			Score:  1,
	//			Member: "one",
	//		},
	//		Key: "zset1",
	//	}))
	//})

	//It("should BZPopMin blocks", func() {
	//	started := make(chan bool)
	//	done := make(chan bool)
	//	go func() {
	//		defer GinkgoRecover()
	//
	//		started <- true
	//		bZPopMin := client.BZPopMin(ctx, 0, "zset")
	//		Expect(bZPopMin.Err()).NotTo(HaveOccurred())
	//		Expect(bZPopMin.Val()).To(Equal(&redis.ZWithKey{
	//			Z: redis.Z{
	//				Member: "a",
	//				Score:  1,
	//			},
	//			Key: "zset",
	//		}))
	//		done <- true
	//	}()
	//	<-started
	//
	//	select {
	//	case <-done:
	//		Fail("BZPopMin is not blocked")
	//	case <-time.After(time.Second):
	//		// ok
	//	}
	//
	//	zAdd := client.ZAdd(ctx, "zset", redis.Z{
	//		Member: "a",
	//		Score:  1,
	//	})
	//	Expect(zAdd.Err()).NotTo(HaveOccurred())
	//
	//	select {
	//	case <-done:
	//		// ok
	//	case <-time.After(time.Second):
	//		Fail("BZPopMin is still blocked")
	//	}
	//})

	//It("should BZPopMin timeout", func() {
	//	val, err := client.BZPopMin(ctx, time.Second, "zset1").Result()
	//	Expect(err).To(Equal(redis.Nil))
	//	Expect(val).To(BeNil())
	//
	//	Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	//
	//	stats := client.PoolStats()
	//	Expect(stats.Hits).To(Equal(uint32(2)))
	//	Expect(stats.Misses).To(Equal(uint32(1)))
	//	Expect(stats.Timeouts).To(Equal(uint32(0)))
	//})

	It("should ZAdd", func() {
		added, err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "uno",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "two",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(0)))

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  1,
			Member: "uno",
		}, {
			Score:  3,
			Member: "two",
		}}))
	})

	It("should ZAdd bytes", func() {
		added, err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: []byte("one"),
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: []byte("uno"),
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: []byte("two"),
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(1)))

		added, err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: []byte("two"),
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(added).To(Equal(int64(0)))

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  1,
			Member: "uno",
		}, {
			Score:  3,
			Member: "two",
		}}))
	})

	//It("should ZAddArgsGTAndLT", func() {
	//	// Test only the GT+LT options.
	//	added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		GT:      true,
	//		Members: []redis.Z{{Score: 1, Member: "one"}},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//
	//	added, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		GT:      true,
	//		Members: []redis.Z{{Score: 2, Member: "one"}},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//
	//	added, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		LT:      true,
	//		Members: []redis.Z{{Score: 1, Member: "one"}},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//})

	//It("should ZAddArgsLT", func() {
	//	added, err := client.ZAddLT(ctx, "zset", redis.Z{
	//		Score:  2,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//
	//	added, err = client.ZAddLT(ctx, "zset", redis.Z{
	//		Score:  3,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//
	//	added, err = client.ZAddLT(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//})

	//It("should ZAddArgsGT", func() {
	//	added, err := client.ZAddGT(ctx, "zset", redis.Z{
	//		Score:  2,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//
	//	added, err = client.ZAddGT(ctx, "zset", redis.Z{
	//		Score:  3,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "one"}}))
	//
	//	added, err = client.ZAddGT(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "one"}}))
	//})

	//It("should ZAddArgsNX", func() {
	//	added, err := client.ZAddNX(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//
	//	added, err = client.ZAddNX(ctx, "zset", redis.Z{
	//		Score:  2,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//})

	//It("should ZAddArgsXX", func() {
	//	added, err := client.ZAddXX(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(BeEmpty())
	//
	//	added, err = client.ZAdd(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	added, err = client.ZAddXX(ctx, "zset", redis.Z{
	//		Score:  2,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//})

	//PIt("should ZAddArgsCh", func() {
	//	changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(1)))
	//
	//	changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(0)))
	//})
	//
	//PIt("should ZAddArgsNXCh", func() {
	//	changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		NX: true,
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//
	//	changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		NX: true,
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 2, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//})
	//
	//PIt("should ZAddArgsXXCh", func() {
	//	changed, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		XX: true,
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(0)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(BeEmpty())
	//
	//	added, err := client.ZAdd(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	changed, err = client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		XX: true,
	//		Ch: true,
	//		Members: []redis.Z{
	//			{Score: 2, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(changed).To(Equal(int64(1)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//})
	//
	//PIt("should ZAddArgsIncr", func() {
	//	score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(score).To(Equal(float64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//
	//	score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(score).To(Equal(float64(2)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//})
	//
	//PIt("should ZAddArgsIncrNX", func() {
	//	score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		NX: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(score).To(Equal(float64(1)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//
	//	score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		NX: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).To(Equal(redis.Nil))
	//	Expect(score).To(Equal(float64(0)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))
	//})
	//
	//PIt("should ZAddArgsIncrXX", func() {
	//	score, err := client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		XX: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).To(Equal(redis.Nil))
	//	Expect(score).To(Equal(float64(0)))
	//
	//	vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(BeEmpty())
	//
	//	added, err := client.ZAdd(ctx, "zset", redis.Z{
	//		Score:  1,
	//		Member: "one",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(1)))
	//
	//	score, err = client.ZAddArgsIncr(ctx, "zset", redis.ZAddArgs{
	//		XX: true,
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(score).To(Equal(float64(2)))
	//
	//	vals, err = client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "one"}}))
	//})

	It("should ZCard", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		card, err := client.ZCard(ctx, "zset").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(card).To(Equal(int64(2)))
	})

	It("should ZCount", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "three",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		count, err := client.ZCount(ctx, "zset", "-inf", "+inf").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(3)))

		count, err = client.ZCount(ctx, "zset", "(1", "3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(2)))

		count, err = client.ZLexCount(ctx, "zset", "-", "+").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(3)))
	})

	It("should ZIncrBy", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		n, err := client.ZIncrBy(ctx, "zset", 2, "one").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(float64(3)))

		val, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]redis.Z{{
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "one",
		}}))
	})

	It("should ZInterStore", func() {
		err := client.ZAdd(ctx, "zset1", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset1", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset3", redis.Z{Score: 3, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())

		n, err := client.ZInterStore(ctx, "out", &redis.ZStore{
			Keys:    []string{"zset1", "zset2"},
			Weights: []float64{2, 3},
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(2)))

		vals, err := client.ZRangeWithScores(ctx, "out", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  5,
			Member: "one",
		}, {
			Score:  10,
			Member: "two",
		}}))
	})

	//It("should ZMPop", func() {
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	key, elems, err := client.ZMPop(ctx, "min", 1, "zset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("zset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//
	//	_, _, err = client.ZMPop(ctx, "min", 1, "nosuchkey").Result()
	//	Expect(err).To(Equal(redis.Nil))
	//
	//	err = client.ZAdd(ctx, "myzset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "myzset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "myzset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	key, elems, err = client.ZMPop(ctx, "min", 1, "myzset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("myzset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//
	//	key, elems, err = client.ZMPop(ctx, "max", 10, "myzset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("myzset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  3,
	//		Member: "three",
	//	}, {
	//		Score:  2,
	//		Member: "two",
	//	}}))
	//
	//	err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 4, Member: "four"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 5, Member: "five"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 6, Member: "six"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	key, elems, err = client.ZMPop(ctx, "min", 10, "myzset", "myzset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("myzset2"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  4,
	//		Member: "four",
	//	}, {
	//		Score:  5,
	//		Member: "five",
	//	}, {
	//		Score:  6,
	//		Member: "six",
	//	}}))
	//})

	//PIt("should BZMPop", func() {
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	key, elems, err := client.BZMPop(ctx, 0, "min", 1, "zset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("zset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//	key, elems, err = client.BZMPop(ctx, 0, "max", 1, "zset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("zset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  3,
	//		Member: "three",
	//	}}))
	//	key, elems, err = client.BZMPop(ctx, 0, "min", 10, "zset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("zset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  2,
	//		Member: "two",
	//	}}))
	//
	//	key, elems, err = client.BZMPop(ctx, 0, "max", 10, "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("zset2"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  3,
	//		Member: "three",
	//	}, {
	//		Score:  2,
	//		Member: "two",
	//	}, {
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//
	//	err = client.ZAdd(ctx, "myzset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	key, elems, err = client.BZMPop(ctx, 0, "min", 10, "myzset").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("myzset"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  1,
	//		Member: "one",
	//	}}))
	//
	//	err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 4, Member: "four"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "myzset2", redis.Z{Score: 5, Member: "five"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	key, elems, err = client.BZMPop(ctx, 0, "min", 10, "myzset", "myzset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(key).To(Equal("myzset2"))
	//	Expect(elems).To(Equal([]redis.Z{{
	//		Score:  4,
	//		Member: "four",
	//	}, {
	//		Score:  5,
	//		Member: "five",
	//	}}))
	//})
	//
	//PIt("should BZMPopBlocks", func() {
	//	started := make(chan bool)
	//	done := make(chan bool)
	//	go func() {
	//		defer GinkgoRecover()
	//
	//		started <- true
	//		key, elems, err := client.BZMPop(ctx, 0, "min", 1, "list_list").Result()
	//		Expect(err).NotTo(HaveOccurred())
	//		Expect(key).To(Equal("list_list"))
	//		Expect(elems).To(Equal([]redis.Z{{
	//			Score:  1,
	//			Member: "one",
	//		}}))
	//		done <- true
	//	}()
	//	<-started
	//
	//	select {
	//	case <-done:
	//		Fail("BZMPop is not blocked")
	//	case <-time.After(time.Second):
	//		// ok
	//	}
	//
	//	err := client.ZAdd(ctx, "list_list", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	select {
	//	case <-done:
	//		// ok
	//	case <-time.After(time.Second):
	//		Fail("BZMPop is still blocked")
	//	}
	//})
	//
	//PIt("should BZMPop timeout", func() {
	//	_, val, err := client.BZMPop(ctx, time.Second, "min", 1, "list1").Result()
	//	Expect(err).To(Equal(redis.Nil))
	//	Expect(val).To(BeNil())
	//
	//	Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	//
	//	stats := client.PoolStats()
	//	Expect(stats.Hits).To(Equal(uint32(2)))
	//	Expect(stats.Misses).To(Equal(uint32(1)))
	//	Expect(stats.Timeouts).To(Equal(uint32(0)))
	//})
	//
	//PIt("should ZMScore", func() {
	//	zmScore := client.ZMScore(ctx, "zset", "one", "three")
	//	Expect(zmScore.Err()).NotTo(HaveOccurred())
	//	Expect(zmScore.Val()).To(HaveLen(2))
	//	Expect(zmScore.Val()[0]).To(Equal(float64(0)))
	//
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	zmScore = client.ZMScore(ctx, "zset", "one", "three")
	//	Expect(zmScore.Err()).NotTo(HaveOccurred())
	//	Expect(zmScore.Val()).To(HaveLen(2))
	//	Expect(zmScore.Val()[0]).To(Equal(float64(1)))
	//
	//	zmScore = client.ZMScore(ctx, "zset", "four")
	//	Expect(zmScore.Err()).NotTo(HaveOccurred())
	//	Expect(zmScore.Val()).To(HaveLen(1))
	//
	//	zmScore = client.ZMScore(ctx, "zset", "four", "one")
	//	Expect(zmScore.Err()).NotTo(HaveOccurred())
	//	Expect(zmScore.Val()).To(HaveLen(2))
	//})

	It("should ZPopMax", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "three",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		members, err := client.ZPopMax(ctx, "zset").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}}))

		// adding back 3
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "three",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		members, err = client.ZPopMax(ctx, "zset", 2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}, {
			Score:  2,
			Member: "two",
		}}))

		// adding back 2 & 3
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "three",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		members, err = client.ZPopMax(ctx, "zset", 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  1,
			Member: "one",
		}}))
		err = client.Do(ctx, "ZPOPMAX", "zset", 1, 2).Err()
		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'zpopmax' command")))
	})

	It("should ZPopMin", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  3,
			Member: "three",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		members, err := client.ZPopMin(ctx, "zset").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}}))

		// adding back 1
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		members, err = client.ZPopMin(ctx, "zset", 2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  2,
			Member: "two",
		}}))

		// adding back 1 & 2
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  1,
			Member: "one",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  2,
			Member: "two",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		members, err = client.ZPopMin(ctx, "zset", 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "three",
		}}))
		err = client.Do(ctx, "ZPOPMIN", "zset", 1, 2).Err()
		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'zpopmin' command")))
	})

	It("should ZRange", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRange := client.ZRange(ctx, "zset", 0, -1)
		Expect(zRange.Err()).NotTo(HaveOccurred())
		Expect(zRange.Val()).To(Equal([]string{"one", "two", "three"}))

		zRange = client.ZRange(ctx, "zset", 2, 3)
		Expect(zRange.Err()).NotTo(HaveOccurred())
		Expect(zRange.Val()).To(Equal([]string{"three"}))

		zRange = client.ZRange(ctx, "zset", -2, -1)
		Expect(zRange.Err()).NotTo(HaveOccurred())
		Expect(zRange.Val()).To(Equal([]string{"two", "three"}))
	})

	It("should ZRangeWithScores", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "three",
		}}))

		vals, err = client.ZRangeWithScores(ctx, "zset", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{Score: 3, Member: "three"}}))

		vals, err = client.ZRangeWithScores(ctx, "zset", -2, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "three",
		}}))
	})

	//It("should ZRangeArgs", func() {
	//	added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//			{Score: 2, Member: "two"},
	//			{Score: 3, Member: "three"},
	//			{Score: 4, Member: "four"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(4)))
	//
	//	zRange, err := client.ZRangeArgs(ctx, redis.ZRangeArgs{
	//		Key:     "zset",
	//		Start:   1,
	//		Stop:    4,
	//		ByScore: true,
	//		Rev:     true,
	//		Offset:  1,
	//		Count:   2,
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(zRange).To(Equal([]string{"three", "two"}))
	//
	//	zRange, err = client.ZRangeArgs(ctx, redis.ZRangeArgs{
	//		Key:    "zset",
	//		Start:  "-",
	//		Stop:   "+",
	//		ByLex:  true,
	//		Rev:    true,
	//		Offset: 2,
	//		Count:  2,
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(zRange).To(Equal([]string{"two", "one"}))
	//
	//	zRange, err = client.ZRangeArgs(ctx, redis.ZRangeArgs{
	//		Key:     "zset",
	//		Start:   "(1",
	//		Stop:    "(4",
	//		ByScore: true,
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(zRange).To(Equal([]string{"two", "three"}))
	//
	//	// withScores.
	//	zSlice, err := client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
	//		Key:     "zset",
	//		Start:   1,
	//		Stop:    4,
	//		ByScore: true,
	//		Rev:     true,
	//		Offset:  1,
	//		Count:   2,
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(zSlice).To(Equal([]redis.Z{
	//		{Score: 3, Member: "three"},
	//		{Score: 2, Member: "two"},
	//	}))
	//})

	It("should ZRangeByScore", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRangeByScore := client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		})
		Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
		Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two", "three"}))

		zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
			Min: "1",
			Max: "2",
		})
		Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
		Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two"}))

		zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
			Min: "(1",
			Max: "2",
		})
		Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
		Expect(zRangeByScore.Val()).To(Equal([]string{"two"}))

		zRangeByScore = client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{
			Min: "(1",
			Max: "(2",
		})
		Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
		Expect(zRangeByScore.Val()).To(Equal([]string{}))
	})

	It("should ZRangeByLex", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{
			Score:  0,
			Member: "a",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  0,
			Member: "b",
		}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{
			Score:  0,
			Member: "c",
		}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRangeByLex := client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
			Min: "-",
			Max: "+",
		})
		Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
		Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b", "c"}))

		zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
			Min: "[a",
			Max: "[b",
		})
		Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
		Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b"}))

		zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
			Min: "(a",
			Max: "[b",
		})
		Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
		Expect(zRangeByLex.Val()).To(Equal([]string{"b"}))

		zRangeByLex = client.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{
			Min: "(a",
			Max: "(b",
		})
		Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
		Expect(zRangeByLex.Val()).To(Equal([]string{}))
	})

	It("should ZRangeByScoreWithScoresMap", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "three",
		}}))

		vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
			Min: "1",
			Max: "2",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  2,
			Member: "two",
		}}))

		vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
			Min: "(1",
			Max: "2",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))

		vals, err = client.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
			Min: "(1",
			Max: "(2",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{}))
	})

	//It("should ZRangeStore", func() {
	//	added, err := client.ZAddArgs(ctx, "zset", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//			{Score: 2, Member: "two"},
	//			{Score: 3, Member: "three"},
	//			{Score: 4, Member: "four"},
	//		},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(added).To(Equal(int64(4)))
	//
	//	rangeStore, err := client.ZRangeStore(ctx, "new-zset", redis.ZRangeArgs{
	//		Key:     "zset",
	//		Start:   1,
	//		Stop:    4,
	//		ByScore: true,
	//		Rev:     true,
	//		Offset:  1,
	//		Count:   2,
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(rangeStore).To(Equal(int64(2)))
	//
	//	zRange, err := client.ZRange(ctx, "new-zset", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(zRange).To(Equal([]string{"two", "three"}))
	//})

	It("should ZRank", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRank := client.ZRank(ctx, "zset", "three")
		Expect(zRank.Err()).NotTo(HaveOccurred())
		Expect(zRank.Val()).To(Equal(int64(2)))

		zRank = client.ZRank(ctx, "zset", "four")
		Expect(zRank.Err()).To(Equal(redis.Nil))
		Expect(zRank.Val()).To(Equal(int64(0)))
	})

	//It("should ZRankWithScore", func() {
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	zRankWithScore := client.ZRankWithScore(ctx, "zset", "one")
	//	Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 0, Score: 1}))
	//
	//	zRankWithScore = client.ZRankWithScore(ctx, "zset", "two")
	//	Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 1, Score: 2}))
	//
	//	zRankWithScore = client.ZRankWithScore(ctx, "zset", "three")
	//	Expect(zRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 2, Score: 3}))
	//
	//	zRankWithScore = client.ZRankWithScore(ctx, "zset", "four")
	//	Expect(zRankWithScore.Err()).To(HaveOccurred())
	//	Expect(zRankWithScore.Err()).To(Equal(redis.Nil))
	//})

	It("should ZRem", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRem := client.ZRem(ctx, "zset", "two")
		Expect(zRem.Err()).NotTo(HaveOccurred())
		Expect(zRem.Val()).To(Equal(int64(1)))

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  1,
			Member: "one",
		}, {
			Score:  3,
			Member: "three",
		}}))
	})

	It("should ZRemRangeByRank", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRemRangeByRank := client.ZRemRangeByRank(ctx, "zset", 0, 1)
		Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
		Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}}))
	})

	It("should perform Case 1: ZRemRangeByRank", func() {
		client.Del(ctx, "zset1")

		vals, err := client.ZRange(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(BeEmpty())

		err = client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "m1"}, redis.Z{Score: 2, Member: "m2"}, redis.Z{Score: 3, Member: "m3"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err = client.ZRange(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"m1", "m2", "m3"}))

		zRemRangeByRank := client.ZRemRangeByRank(ctx, "zset1", 0, 1)
		Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
		Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

		vals, err = client.ZRange(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).NotTo(BeEmpty())
	})

	It("should perform Case 2: ZRemRangeByRank", func() {
		client.Del(ctx, "zset1")

		err := client.ZAdd(ctx, "zset1", redis.Z{Score: 3, Member: "m3"}, redis.Z{Score: 4, Member: "m4"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRange(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"m3", "m4"}))

		zRemRangeByRank := client.ZRemRangeByRank(ctx, "zset1", 0, 1)
		Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
		Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

		vals, err = client.ZRange(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(BeEmpty())
	})

	It("should perform Case 3: ZRemRangeByRank", func() {
		client.Del(ctx, "zset1")

		err := client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "m2"}, redis.Z{Score: 3, Member: "m1"},
			redis.Z{Score: 3, Member: "m3"}, redis.Z{Score: 4, Member: "m4"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRangeWithScores(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{
			{Score: 2, Member: "m2"},
			{Score: 3, Member: "m1"},
			{Score: 3, Member: "m3"},
			{Score: 4, Member: "m4"},
		}))

		zRemRangeByRank := client.ZRemRangeByRank(ctx, "zset1", 0, 1)
		Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
		Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

		vals, err = client.ZRangeWithScores(ctx, "zset1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{
			{Score: 3, Member: "m3"},
			{Score: 4, Member: "m4"},
		}))
	})

	It("should ZRemRangeByScore", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRemRangeByScore := client.ZRemRangeByScore(ctx, "zset", "-inf", "(2")
		Expect(zRemRangeByScore.Err()).NotTo(HaveOccurred())
		Expect(zRemRangeByScore.Val()).To(Equal(int64(1)))

		vals, err := client.ZRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  2,
			Member: "two",
		}, {
			Score:  3,
			Member: "three",
		}}))
	})

	It("should ZRemRangeByLex", func() {
		zz := []redis.Z{
			{Score: 0, Member: "aaaa"},
			{Score: 0, Member: "b"},
			{Score: 0, Member: "c"},
			{Score: 0, Member: "d"},
			{Score: 0, Member: "e"},
			{Score: 0, Member: "foo"},
			{Score: 0, Member: "zap"},
			{Score: 0, Member: "zip"},
			{Score: 0, Member: "ALPHA"},
			{Score: 0, Member: "alpha"},
		}
		for _, z := range zz {
			err := client.ZAdd(ctx, "zset", z).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		n, err := client.ZRemRangeByLex(ctx, "zset", "[alpha", "[omega").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(6)))

		vals, err := client.ZRange(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"ALPHA", "aaaa", "zap", "zip"}))
	})

	It("should ZRevRange", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRevRange := client.ZRevRange(ctx, "zset", 0, -1)
		Expect(zRevRange.Err()).NotTo(HaveOccurred())
		Expect(zRevRange.Val()).To(Equal([]string{"three", "two", "one"}))

		zRevRange = client.ZRevRange(ctx, "zset", 2, 3)
		Expect(zRevRange.Err()).NotTo(HaveOccurred())
		Expect(zRevRange.Val()).To(Equal([]string{"one"}))

		zRevRange = client.ZRevRange(ctx, "zset", -2, -1)
		Expect(zRevRange.Err()).NotTo(HaveOccurred())
		Expect(zRevRange.Val()).To(Equal([]string{"two", "one"}))
	})

	It("should ZRevRangeWithScoresMap", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := client.ZRevRangeWithScores(ctx, "zset", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  1,
			Member: "one",
		}}))

		val, err = client.ZRevRangeWithScores(ctx, "zset", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

		val, err = client.ZRevRangeWithScores(ctx, "zset", -2, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]redis.Z{{
			Score:  2,
			Member: "two",
		}, {
			Score:  1,
			Member: "one",
		}}))
	})

	It("should ZRevRangeByScore", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRevRangeByScore(
			ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"three", "two", "one"}))

		vals, err = client.ZRevRangeByScore(
			ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"two"}))

		vals, err = client.ZRevRangeByScore(
			ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{}))
	})

	It("should ZRevRangeByLex", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "a"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "b"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 0, Member: "c"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRevRangeByLex(
			ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "-"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"c", "b", "a"}))

		vals, err = client.ZRevRangeByLex(
			ctx, "zset", &redis.ZRangeBy{Max: "[b", Min: "(a"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{"b"}))

		vals, err = client.ZRevRangeByLex(
			ctx, "zset", &redis.ZRangeBy{Max: "(b", Min: "(a"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]string{}))
	})

	It("should ZRevRangeByScoreWithScores", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRevRangeByScoreWithScores(
			ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  1,
			Member: "one",
		}}))
	})

	It("should ZRevRangeByScoreWithScoresMap", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		vals, err := client.ZRevRangeByScoreWithScores(
			ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{
			Score:  3,
			Member: "three",
		}, {
			Score:  2,
			Member: "two",
		}, {
			Score:  1,
			Member: "one",
		}}))

		vals, err = client.ZRevRangeByScoreWithScores(
			ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))

		vals, err = client.ZRevRangeByScoreWithScores(
			ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(Equal([]redis.Z{}))
	})

	It("should ZRevRank", func() {
		err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		zRevRank := client.ZRevRank(ctx, "zset", "one")
		Expect(zRevRank.Err()).NotTo(HaveOccurred())
		Expect(zRevRank.Val()).To(Equal(int64(2)))

		zRevRank = client.ZRevRank(ctx, "zset", "four")
		Expect(zRevRank.Err()).To(Equal(redis.Nil))
		Expect(zRevRank.Val()).To(Equal(int64(0)))
	})

	//It("should ZRevRankWithScore", func() {
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	zRevRankWithScore := client.ZRevRankWithScore(ctx, "zset", "one")
	//	Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 2, Score: 1}))
	//
	//	zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "two")
	//	Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 1, Score: 2}))
	//
	//	zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "three")
	//	Expect(zRevRankWithScore.Err()).NotTo(HaveOccurred())
	//	Expect(zRevRankWithScore.Result()).To(Equal(redis.RankScore{Rank: 0, Score: 3}))
	//
	//	zRevRankWithScore = client.ZRevRankWithScore(ctx, "zset", "four")
	//	Expect(zRevRankWithScore.Err()).To(HaveOccurred())
	//	Expect(zRevRankWithScore.Err()).To(Equal(redis.Nil))
	//})

	It("should ZScore", func() {
		zAdd := client.ZAdd(ctx, "zset", redis.Z{Score: 1.001, Member: "one"})
		Expect(zAdd.Err()).NotTo(HaveOccurred())

		zScore := client.ZScore(ctx, "zset", "one")
		Expect(zScore.Err()).NotTo(HaveOccurred())
		Expect(zScore.Val()).To(Equal(1.001))
	})

	//It("should ZUnion", func() {
	//	err := client.ZAddArgs(ctx, "zset1", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//			{Score: 2, Member: "two"},
	//		},
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	err = client.ZAddArgs(ctx, "zset2", redis.ZAddArgs{
	//		Members: []redis.Z{
	//			{Score: 1, Member: "one"},
	//			{Score: 2, Member: "two"},
	//			{Score: 3, Member: "three"},
	//		},
	//	}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	union, err := client.ZUnion(ctx, redis.ZStore{
	//		Keys:      []string{"zset1", "zset2"},
	//		Weights:   []float64{2, 3},
	//		Aggregate: "sum",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(union).To(Equal([]string{"one", "three", "two"}))
	//
	//	unionScores, err := client.ZUnionWithScores(ctx, redis.ZStore{
	//		Keys:      []string{"zset1", "zset2"},
	//		Weights:   []float64{2, 3},
	//		Aggregate: "sum",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(unionScores).To(Equal([]redis.Z{
	//		{Score: 5, Member: "one"},
	//		{Score: 9, Member: "three"},
	//		{Score: 10, Member: "two"},
	//	}))
	//})

	It("should ZUnionStore", func() {
		err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
		Expect(err).NotTo(HaveOccurred())

		n, err := client.ZUnionStore(ctx, "out", &redis.ZStore{
			Keys:    []string{"zset1", "zset2"},
			Weights: []float64{2, 3},
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(3)))

		val, err := client.ZRangeWithScores(ctx, "out", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]redis.Z{{
			Score:  5,
			Member: "one",
		}, {
			Score:  9,
			Member: "three",
		}, {
			Score:  10,
			Member: "two",
		}}))
	})

	It("should  ZREVRANK", func() {
		err := client.ZAdd(ctx, "key", redis.Z{Score: 100, Member: "a1b2C3d4E5"}).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.Del(ctx, "key").Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.ZAdd(ctx, "key", redis.Z{Score: 101, Member: "F6g7H8i9J0"}).Err()
		Expect(err).NotTo(HaveOccurred())

		rank, err := client.ZRank(ctx, "key", "a1b2C3d4E5").Result()
		Expect(err).To(Equal(redis.Nil))
		Expect(rank).To(Equal(int64(0)))

		revrank, err := client.ZRevRank(ctx, "key", "a1b2C3d4E5").Result()
		Expect(err).To(Equal(redis.Nil))
		Expect(revrank).To(Equal(int64(0)))

		scanResult, cursor, err := client.ZScan(ctx, "key", 0, "", 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(cursor).To(Equal(uint64(0)))
		Expect(scanResult).To(Equal([]string{"F6g7H8i9J0", "101"}))

		card, err := client.ZCard(ctx, "key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(card).To(Equal(int64(1)))
	})

	//It("should ZRandMember", func() {
	//	err := client.ZAdd(ctx, "zset", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	v := client.ZRandMember(ctx, "zset", 1)
	//	Expect(v.Err()).NotTo(HaveOccurred())
	//	Expect(v.Val()).To(Or(Equal([]string{"one"}), Equal([]string{"two"})))
	//
	//	v = client.ZRandMember(ctx, "zset", 0)
	//	Expect(v.Err()).NotTo(HaveOccurred())
	//	Expect(v.Val()).To(HaveLen(0))
	//
	//	kv, err := client.ZRandMemberWithScores(ctx, "zset", 1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(kv).To(Or(
	//		Equal([]redis.Z{{Member: "one", Score: 1}}),
	//		Equal([]redis.Z{{Member: "two", Score: 2}}),
	//	))
	//})

	//It("should ZDiff", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	v, err := client.ZDiff(ctx, "zset1", "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal([]string{"two", "three"}))
	//})

	//It("should ZDiffWithScores", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	v, err := client.ZDiffWithScores(ctx, "zset1", "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal([]redis.Z{
	//		{
	//			Member: "two",
	//			Score:  2,
	//		},
	//		{
	//			Member: "three",
	//			Score:  3,
	//		},
	//	}))
	//})

	//It("should ZInter", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	v, err := client.ZInter(ctx, &redis.ZStore{
	//		Keys: []string{"zset1", "zset2"},
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal([]string{"one", "two"}))
	//})

	//It("should ZInterCard", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	// limit 0 means no limit
	//	sInterCard := client.ZInterCard(ctx, 0, "zset1", "zset2")
	//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
	//	Expect(sInterCard.Val()).To(Equal(int64(2)))
	//
	//	sInterCard = client.ZInterCard(ctx, 1, "zset1", "zset2")
	//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
	//	Expect(sInterCard.Val()).To(Equal(int64(1)))
	//
	//	sInterCard = client.ZInterCard(ctx, 3, "zset1", "zset2")
	//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
	//	Expect(sInterCard.Val()).To(Equal(int64(2)))
	//})
	//
	//It("should ZInterWithScores", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	v, err := client.ZInterWithScores(ctx, &redis.ZStore{
	//		Keys:      []string{"zset1", "zset2"},
	//		Weights:   []float64{2, 3},
	//		Aggregate: "Max",
	//	}).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal([]redis.Z{
	//		{
	//			Member: "one",
	//			Score:  3,
	//		},
	//		{
	//			Member: "two",
	//			Score:  6,
	//		},
	//	}))
	//})

	//It("should ZDiffStore", func() {
	//	err := client.ZAdd(ctx, "zset1", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset1", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "one"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 2, Member: "two"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	err = client.ZAdd(ctx, "zset2", redis.Z{Score: 3, Member: "three"}).Err()
	//	Expect(err).NotTo(HaveOccurred())
	//	v, err := client.ZDiffStore(ctx, "out1", "zset1", "zset2").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal(int64(0)))
	//	v, err = client.ZDiffStore(ctx, "out1", "zset2", "zset1").Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(v).To(Equal(int64(1)))
	//	vals, err := client.ZRangeWithScores(ctx, "out1", 0, -1).Result()
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(vals).To(Equal([]redis.Z{{
	//		Score:  3,
	//		Member: "three",
	//	}}))
	//})
})
