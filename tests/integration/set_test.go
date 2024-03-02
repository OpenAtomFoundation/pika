package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Set Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("sets", func() {
		It("should SAdd", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SAdd strings", func() {
			set := []string{"Hello", "World", "World"}
			sAdd := client.SAdd(ctx, "set", set)
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(2)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SCard", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sCard := client.SCard(ctx, "set")
			Expect(sCard.Err()).NotTo(HaveOccurred())
			Expect(sCard.Val()).To(Equal(int64(2)))
		})

		It("should SDiff", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sDiff := client.SDiff(ctx, "set1", "set2")
			Expect(sDiff.Err()).NotTo(HaveOccurred())
			Expect(sDiff.Val()).To(ConsistOf([]string{"a", "b"}))

			sDiff = client.SDiff(ctx, "nonexistent_set1", "nonexistent_set2")
			Expect(sDiff.Err()).NotTo(HaveOccurred())
			Expect(sDiff.Val()).To(HaveLen(0))
		})

		It("should SDiffStore", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sDiffStore := client.SDiffStore(ctx, "set", "set1", "set2")
			Expect(sDiffStore.Err()).NotTo(HaveOccurred())
			Expect(sDiffStore.Val()).To(Equal(int64(2)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"a", "b"}))
		})

		It("should SInter", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sInter := client.SInter(ctx, "set1", "set2")
			Expect(sInter.Err()).NotTo(HaveOccurred())
			Expect(sInter.Val()).To(Equal([]string{"c"}))

			sInter = client.SInter(ctx, "nonexistent_set1", "nonexistent_set2")
			Expect(sInter.Err()).NotTo(HaveOccurred())
			Expect(sInter.Val()).To(HaveLen(0))
		})

		//It("should SInterCard", func() {
		//	sAdd := client.SAdd(ctx, "set1", "a")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	sAdd = client.SAdd(ctx, "set1", "b")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	sAdd = client.SAdd(ctx, "set1", "c")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//
		//	sAdd = client.SAdd(ctx, "set2", "b")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	sAdd = client.SAdd(ctx, "set2", "c")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	sAdd = client.SAdd(ctx, "set2", "d")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	sAdd = client.SAdd(ctx, "set2", "e")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//	// limit 0 means no limit,see https://redis.io/commands/sintercard/ for more details
		//	sInterCard := client.SInterCard(ctx, 0, "set1", "set2")
		//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
		//	Expect(sInterCard.Val()).To(Equal(int64(2)))
		//
		//	sInterCard = client.SInterCard(ctx, 1, "set1", "set2")
		//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
		//	Expect(sInterCard.Val()).To(Equal(int64(1)))
		//
		//	sInterCard = client.SInterCard(ctx, 3, "set1", "set2")
		//	Expect(sInterCard.Err()).NotTo(HaveOccurred())
		//	Expect(sInterCard.Val()).To(Equal(int64(2)))
		//})

		It("should SInterStore", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sInterStore := client.SInterStore(ctx, "set", "set1", "set2")
			Expect(sInterStore.Err()).NotTo(HaveOccurred())
			Expect(sInterStore.Val()).To(Equal(int64(1)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(Equal([]string{"c"}))
		})

		It("should IsMember", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sIsMember := client.SIsMember(ctx, "set", "one")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(true))

			sIsMember = client.SIsMember(ctx, "set", "two")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(false))
		})

		//It("should SMIsMember", func() {
		//	sAdd := client.SAdd(ctx, "set", "one")
		//	Expect(sAdd.Err()).NotTo(HaveOccurred())
		//
		//	sMIsMember := client.SMIsMember(ctx, "set", "one", "two")
		//	Expect(sMIsMember.Err()).NotTo(HaveOccurred())
		//	Expect(sMIsMember.Val()).To(Equal([]bool{true, false}))
		//})

		It("should SMembers", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SMembersMap", func() {
			sAdd := client.SAdd(ctx, "set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMembersMap := client.SMembersMap(ctx, "set")
			Expect(sMembersMap.Err()).NotTo(HaveOccurred())
			Expect(sMembersMap.Val()).To(Equal(map[string]struct{}{"Hello": {}, "World": {}}))
		})

		It("should SMove", func() {
			sAdd := client.SAdd(ctx, "set1", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMove := client.SMove(ctx, "set1", "set2", "two")
			Expect(sMove.Err()).NotTo(HaveOccurred())
			Expect(sMove.Val()).To(Equal(true))

			sMembers := client.SMembers(ctx, "set1")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(Equal([]string{"one"}))

			sMembers = client.SMembers(ctx, "set2")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"three", "two"}))
		})

		It("should SPop", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			// 报错：redis: can't parse reply="*1" reading string
			//sPop := client.SPop(ctx, "set")
			//Expect(sPop.Err()).NotTo(HaveOccurred())
			//Expect(sPop.Val()).NotTo(Equal(""))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(3))

			err := client.Do(ctx, "SPOP", "set", 1, 2).Err()
			Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'spop' command")))
		})

		It("should SPopN", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "four")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sPopN := client.SPopN(ctx, "set", 1)
			Expect(sPopN.Err()).NotTo(HaveOccurred())
			Expect(sPopN.Val()).NotTo(Equal([]string{""}))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(3))

			sPopN = client.SPopN(ctx, "set", 4)
			Expect(sPopN.Err()).NotTo(HaveOccurred())
			Expect(sPopN.Val()).To(HaveLen(3))

			sMembers = client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(0))
		})

		It("should SRandMember and SRandMemberN", func() {
			err := client.SAdd(ctx, "set", "one").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd(ctx, "set", "two").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd(ctx, "set", "three").Err()
			Expect(err).NotTo(HaveOccurred())

			members, err := client.SMembers(ctx, "set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(3))

			member, err := client.SRandMember(ctx, "set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(member).NotTo(Equal(""))

			members, err = client.SRandMemberN(ctx, "set", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(2))
		})

		It("should SRem", func() {
			sAdd := client.SAdd(ctx, "set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sRem := client.SRem(ctx, "set", "one")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(1)))

			sRem = client.SRem(ctx, "set", "four")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"three", "two"}))

			sRem = client.SRem(ctx, "nonexistent_set", "one")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(0)))
		})

		It("should SUnion", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sUnion := client.SUnion(ctx, "set1", "set2")
			Expect(sUnion.Err()).NotTo(HaveOccurred())
			Expect(sUnion.Val()).To(HaveLen(5))

			sUnion = client.SUnion(ctx, "nonexistent_set1", "nonexistent_set2")
			Expect(sUnion.Err()).NotTo(HaveOccurred())
			Expect(sUnion.Val()).To(HaveLen(0))
		})

		It("should SUnionStore", func() {
			sAdd := client.SAdd(ctx, "set1", "a")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "b")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set1", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sAdd = client.SAdd(ctx, "set2", "c")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "d")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd(ctx, "set2", "e")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sUnionStore := client.SUnionStore(ctx, "set", "set1", "set2")
			Expect(sUnionStore.Err()).NotTo(HaveOccurred())
			Expect(sUnionStore.Val()).To(Equal(int64(5)))

			sMembers := client.SMembers(ctx, "set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(5))
		})
	})
})
