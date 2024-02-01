// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package pika_integration

import (
	"sync"
	"context"
	"sync/atomic"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

func streamCompareID(a, b string) int {
	if a == b {
		return 0
	}
	aParts := strings.Split(a, "-")
	bParts := strings.Split(b, "-")
	aMs, _ := strconv.ParseInt(aParts[0], 10, 64)
	aSeq, _ := strconv.ParseInt(aParts[1], 10, 64)
	bMs, _ := strconv.ParseInt(bParts[0], 10, 64)
	bSeq, _ := strconv.ParseInt(bParts[1], 10, 64)
	if aMs > bMs {
		return 1
	}
	if aMs < bMs {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	return 0
}

func streamNextID(id string) string {
	parts := strings.Split(id, "-")
	ms := parts[0]
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	seq++
	return fmt.Sprintf("%s-%d", ms, seq)
}

func streamRandomID(minID, maxID string) string {
	minParts := strings.Split(minID, "-")
	maxParts := strings.Split(maxID, "-")
	minMs, _ := strconv.Atoi(minParts[0])
	maxMs, _ := strconv.Atoi(maxParts[0])
	delta := maxMs - minMs + 1
	ms := minMs + randomInt4Stream(delta)
	seq := randomInt4Stream(1000)
	return fmt.Sprintf("%d-%d", ms, seq)
}

func streamSimulateXRANGE(items []redis.XMessage, start, end string) []redis.XMessage {
	result := make([]redis.XMessage, 0)
	for _, item := range items {
		if streamCompareID(item.ID, start) >= 0 && streamCompareID(item.ID, end) <= 0 {
			result = append(result, item)
		}
	}
	return result
}

// Helper function for generating random integers
func randomInt4Stream(n int) int {
	return rand.Intn(n)
}

func insertIntoStreamKey(client *redis.Client, key string) error {
	count := 1000

	pipe := client.Pipeline()
	for j := 0; j < count; j++ {
		if rand.Float64() < 0.9 {
			pipe.XAdd(context.TODO(), &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": j},
			})
		} else {
			pipe.XAdd(context.TODO(), &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": j, "otherfield": "foo"},
			})
		}
	}

	_, err := pipe.Exec(context.TODO())
	return err
}

func ReverseSlice(slice []redis.XMessage) {
	length := len(slice)
	for i := 0; i < length/2; i++ {
		slice[i], slice[length-i-1] = slice[length-i-1], slice[i]
	}
}

func parseStreamEntryID(id string) (ts int64, seqNum int64) {
	values := strings.Split(id, "-")

	ts, _ = strconv.ParseInt(values[0], 10, 64)
	seqNum, _ = strconv.ParseInt(values[1], 10, 64)

	return
}

var _ = Describe("Stream Commands", func() {
	ctx := context.TODO()
	var client *redis.Client
	client = redis.NewClient(pikaOptions1())
	client.FlushDB(ctx)

	BeforeEach(func() {
		// client = redis.NewClient(pikaOptions1())
		// Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		// Expect(client.Close()).NotTo(HaveOccurred())
		// Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
	})

	Describe("passed tests", func() {
		It("should concurrently add and read messages in the stream with separate clients", func() {
			const streamKey = "mystream"
			const totalMessages = 200
			const numWriters = 10
			const numReaders = 10
			const messagesPerWriter = 20
		
			createClient := func() *redis.Client {
				return redis.NewClient(pikaOptions1())
			}
		
			var messageCount int32
		
			// Start writer goroutines
			for i := 0; i < numWriters; i++ {
				go func(writerIndex int) {
					defer GinkgoRecover()
					writerClient := createClient()
					defer writerClient.Close()
		
					for j := 0; j < messagesPerWriter; j++ {
						_, err := writerClient.XAdd(ctx, &redis.XAddArgs{
							Stream: streamKey,
							Values: map[string]interface{}{"item": writerIndex*messagesPerWriter + j},
						}).Result()
						Expect(err).NotTo(HaveOccurred())
						atomic.AddInt32(&messageCount, 1)
					}
				}(i)
			}
		
			// Start reader goroutines
			var wg sync.WaitGroup
			for i := 0; i < numReaders; i++ {
					wg.Add(1)
					go func() {
							defer GinkgoRecover()
							defer wg.Done()
							readerClient := createClient()
							defer readerClient.Close()

							lastID := "0"
							readMessages := 0
							for readMessages < totalMessages {
									items, err := readerClient.XRead(ctx, &redis.XReadArgs{
											Streams: []string{streamKey, lastID},
											Block:   0,
									}).Result()
									if (err != nil) {
											continue
									}

									// Check if items slice is not empty
									if len(items) > 0 && len(items[0].Messages) > 0 {
											lastMessageIndex := len(items[0].Messages) - 1
											lastID = items[0].Messages[lastMessageIndex].ID
											readMessages += len(items[0].Messages)
									}
									// Optionally add a short delay here if needed
							}
							Expect(readMessages).To(BeNumerically(">=", totalMessages))
					}()
			}

		
			wg.Wait()
			Eventually(func() int32 {
				return atomic.LoadInt32(&messageCount)
			}).Should(Equal(int32(totalMessages)))
		})

		It("should create a stream and find it using keys * command", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			// Creating a stream and adding entries
			_, err := client.XAdd(ctx, &redis.XAddArgs{
					Stream: "mystream",
					ID:     "*",
					Values: map[string]interface{}{"key1": "value1", "key2": "value2"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
	
			// Using keys * to find all keys including the stream
			keys, err := client.Keys(ctx, "*").Result()
			Expect(err).NotTo(HaveOccurred())
	
			// Checking if the stream 'mystream' exists in the returned keys
			found := false
			for _, key := range keys {
					if key == "mystream" {
							found = true
							break
					}
			}
			Expect(found).To(BeTrue(), "Stream 'mystream' should exist in keys")
		})
	
		
		
		It("XADD wrong number of args", func() {
			_, err := client.Do(ctx, "XADD", "mystream").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))

			_, err = client.Do(ctx, "XADD", "mystream", "*").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))

			_, err = client.Do(ctx, "XADD", "mystream", "*", "field").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))
		})

		It("XADD can add entries into a stream that XRANGE can fetch", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err()).NotTo(HaveOccurred())

			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(2)))

			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(len(items)).To(Equal(2))
			Expect(items[0].Values).To(Equal(map[string]interface{}{"item": "1", "value": "a"}))
			Expect(items[1].Values).To(Equal(map[string]interface{}{"item": "2", "value": "b"}))
		})

		It("XADD IDs are incremental", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			id1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()
			id2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Val()
			id3 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Val()

			Expect(streamCompareID(id1, id2)).To(Equal(-1))
			Expect(streamCompareID(id2, id3)).To(Equal(-1))
		})

		It("XADD IDs are incremental when ms is the same as well", func() {
			last_id, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Result()
			Expect(err).NotTo(HaveOccurred())
			for i := 0; i < 100; i++ {
				cur_id := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()
				Expect(streamCompareID(last_id, cur_id)).To(Equal(-1))
				last_id = cur_id
			}
		})

		It("XADD IDs correctly report an error when overflowing", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "18446744073709551615-18446744073709551615", Values: []string{"a", "b"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "*", Values: []string{"c", "d"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		It("XADD auto-generated sequence is incremented for last ID", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			x1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
			x2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-*", Values: []string{"item", "2", "value", "b"}}).Val()
			Expect(x2).To(Equal("123-457"))
			Expect(streamCompareID(x1, x2)).To(Equal(-1))
		})

		It("XADD auto-generated sequence is zero for future timestamp ID", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			x1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
			x2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "789-*", Values: []string{"item", "2", "value", "b"}}).Val()
			Expect(x2).To(Equal("789-0"))
			Expect(streamCompareID(x1, x2)).To(Equal(-1))
		})

		It("XADD auto-generated sequence can't be smaller than last ID", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "42-*", Values: []string{"item", "2", "value", "b"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		It("XADD auto-generated sequence can't overflow", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-18446744073709551615", Values: []string{"a", "b"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-*", Values: []string{"c", "d"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		It("XADD 0-* should succeed", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			x := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "0-*", Values: []string{"a", "b"}}).Val()
			Expect(x).To(Equal("0-1"))
		})

		It("XADD with MAXLEN option", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				if rand.Float64() < 0.9 {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"xitem", strconv.Itoa(i)}}).Err()).NotTo(HaveOccurred())
				} else {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"yitem", strconv.Itoa(i)}}).Err()).NotTo(HaveOccurred())
				}
			}
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(5)))
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			expected := 995
			for _, item := range items {
				Expect(item.Values).To(SatisfyAny(
					HaveKeyWithValue("xitem", strconv.Itoa(expected)),
					HaveKeyWithValue("yitem", strconv.Itoa(expected)),
				))
				expected++
			}
		})

		It("XADD with MAXLEN option and the '=' argument", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				if rand.Float64() < 0.9 {
					Expect(client.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "xitem", "i").Err()).NotTo(HaveOccurred())
				} else {
					Expect(client.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "yitem", "i").Err()).NotTo(HaveOccurred())
				}
			}
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(5)))
		})

		It("XADD with NOMKSTREAM option", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "1", "value", "a"}}).Val()).To(BeEmpty())
			Expect(client.Exists(ctx, "mystream").Val()).To(BeZero())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()).NotTo(BeEmpty())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "2", "value", "b"}}).Val()).NotTo(BeEmpty())
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(2)))
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0].Values).To(Equal(map[string]interface{}{"item": "1", "value": "a"}))
			Expect(items[1].Values).To(Equal(map[string]interface{}{"item": "2", "value": "b"}))
		})

		It("XADD with MINID option", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			buildXAddArgs := func(id int, tag string) *redis.XAddArgs {
				c := id - 5
				if c < 0 {
					c = 1000
				}
				return &redis.XAddArgs{Stream: "mystream", MinID: strconv.Itoa(c), ID: strconv.Itoa(id), Values: []string{"xitem", strconv.Itoa(id)}}
			}
			for i := 0; i < 1000; i++ {
				if rand.Float64() < 0.9 {
					Expect(client.XAdd(ctx, buildXAddArgs(i+1, "xitem")).Err()).NotTo(HaveOccurred())
				} else {
					Expect(client.XAdd(ctx, buildXAddArgs(i+1, "yitem")).Err()).NotTo(HaveOccurred())
				}
			}
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(6)))
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			expected := 995
			for _, item := range items {
				Expect(item.Values).To(SatisfyAny(
					HaveKeyWithValue("xitem", strconv.Itoa(expected)),
					HaveKeyWithValue("yitem", strconv.Itoa(expected)),
				))
				expected++
			}
		})

		It("XTRIM with MINID option", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "2-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "3-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "4-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "5-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XTrimMinID(ctx, "mystream", "3-0").Err()).NotTo(HaveOccurred())
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(items).To(HaveLen(3))
			Expect(items[0].ID).To(Equal("3-0"))
			Expect(items[1].ID).To(Equal("4-0"))
			Expect(items[2].ID).To(Equal("5-0"))
		})

		It("XTRIM with MINID option, big delta from master record", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-0", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-1", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XTrimMinID(ctx, "mystream", "1641544570597-0").Err()).NotTo(HaveOccurred())
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0].ID).To(Equal("1641544570597-0"))
			Expect(items[1].ID).To(Equal("1641544570597-1"))
		})

		It("XADD mass insertion and XLEN", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			insertIntoStreamKey(client, "mystream")
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(len(items)).To(Equal(1000))
			for i := 0; i < 1000; i++ {
				Expect(items[i].Values).To(HaveKeyWithValue("item", strconv.Itoa(i)))
			}
		})

		It("XADD with ID 0-0", func() {
			Expect(client.Del(ctx, "tmpstream").Err()).NotTo(HaveOccurred())
			err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "tmpstream",
				ID:     "0-0",
				Values: []string{"k", "v"},
			}).Err()
			Expect(err).To(HaveOccurred())
			Expect(client.Exists(ctx, "tmpstream").Val()).To(Equal(int64(0)))
		})

		It("XRANGE COUNT works as expected", func() {
			Expect(len(client.XRangeN(ctx, "mystream", "-", "+", 10).Val())).To(Equal(10))
		})

		It("XREVRANGE COUNT works as expected", func() {
			Expect(len(client.XRevRangeN(ctx, "mystream", "+", "-", 10).Val())).To(Equal(10))
		})

		It("XRANGE can be used to iterate the whole stream", func() {
			lastID, c := "-", 0
			for {
				items := client.XRangeN(ctx, "mystream", lastID, "+", 100).Val()
				if len(items) == 0 {
					break
				}
				for _, item := range items {
					Expect(item.Values).To(HaveKeyWithValue("item", strconv.Itoa(c)))
					c++
				}
				lastID = streamNextID(items[len(items)-1].ID)
			}
			Expect(c).To(Equal(1000))
		})

		It("XREVRANGE returns the reverse of XRANGE", func() {
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			revItems := client.XRevRange(ctx, "mystream", "+", "-").Val()
			ReverseSlice(revItems)
			Expect(revItems).To(Equal(items))
		})

		It("XRANGE exclusive ranges", func() {
			ids := []string{"0-1", "0-18446744073709551615", "1-0", "42-0", "42-42", "18446744073709551615-18446744073709551614", "18446744073709551615-18446744073709551615"}
			total := len(ids)
			Expect(client.Do(ctx, "DEL", "tmpstream").Err()).NotTo(HaveOccurred())
			for _, id := range ids {
				Expect(client.XAdd(ctx, &redis.XAddArgs{
					Stream: "tmpstream",
					ID:     id,
					Values: []string{"foo", "bar"},
				}).Err()).NotTo(HaveOccurred())
			}
			// Expect(len(client.XRange(ctx, "tmpstream", "-", "+").Val())).To(Equal(total))
			Expect(len(client.XRange(ctx, "tmpstream", "("+ids[0], "+").Val())).To(Equal(total - 1))
			Expect(len(client.XRange(ctx, "tmpstream", "-", "("+ids[total-1]).Val())).To(Equal(total - 1))
			Expect(len(client.XRange(ctx, "tmpstream", "(0-1", "(1-0").Val())).To(Equal(1))
			Expect(len(client.XRange(ctx, "tmpstream", "(1-0", "(42-42").Val())).To(Equal(1))
			Expect(client.XRange(ctx, "tmpstream", "(-", "+").Err()).To(MatchError(ContainSubstring("ERR")))
			Expect(client.XRange(ctx, "tmpstream", "-", "(+").Err()).To(MatchError(ContainSubstring("ERR")))
			Expect(client.XRange(ctx, "tmpstream", "(18446744073709551615-18446744073709551615", "+").Err()).To(MatchError(ContainSubstring("ERR")))
			Expect(client.XRange(ctx, "tmpstream", "-", "(0-0").Err()).To(MatchError(ContainSubstring("ERR")))
		})

		It("XREAD with non empty stream", func() {
			res, err := client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{"mystream", "0-0"},
				Count:   1,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(Equal(1))
			Expect(res[0].Stream).To(Equal("mystream"))
			Expect(len(res[0].Messages)).To(Equal(1))
			Expect(res[0].Messages[0].Values).To(HaveKeyWithValue("item", "0"))
		})

		It("Non blocking XREAD with empty streams", func() {
			// go-redis blocks underneath; fallback to Do
			Expect(client.Do(ctx, "XREAD", "STREAMS", "s1", "s2", "0-0", "0-0").Val()).To(BeNil())
		})

		It("XREAD with non empty second stream", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "0-1", Values: []string{"item", "0"}}).Err()).NotTo(HaveOccurred())
			r := client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{"nostream", "mystream", "0-0", "0-0"},
				Count:   1,
			}).Val()
			Expect(len(r)).To(Equal(1))
			Expect(r[0].Stream).To(Equal("mystream"))
			Expect(len(r[0].Messages)).To(Equal(1))
			Expect(r[0].Messages[0].Values).To(HaveKeyWithValue("item", "0"))
		})

		It("XDEL basic test", func() {
			Expect(client.Del(ctx, "somestream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value0"}}).Err()).NotTo(HaveOccurred())
			id := client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value1"}}).Val()
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value2"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XDel(ctx, "somestream", id).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "somestream").Val()).To(Equal(int64(2)))
			items := client.XRange(ctx, "somestream", "-", "+").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0].Values).To(SatisfyAny(HaveKeyWithValue("foo", "value0")))
			Expect(items[1].Values).To(SatisfyAny(HaveKeyWithValue("foo", "value2")))
		})

		It("XDEL multiply id test", func() {
			Expect(client.Del(ctx, "somestream").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", ID: "1-1", Values: []string{"a", "1"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", ID: "1-2", Values: []string{"b", "2"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", ID: "1-3", Values: []string{"c", "3"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", ID: "1-4", Values: []string{"d", "4"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", ID: "1-5", Values: []string{"e", "5"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "somestream").Val()).To(Equal(int64(5)))
			Expect(client.XDel(ctx, "somestream", "1-1", "1-4", "1-5", "2-1").Val()).To(Equal(int64(3)))
			Expect(client.XLen(ctx, "somestream").Val()).To(Equal(int64(2)))
			result := client.XRange(ctx, "somestream", "-", "+").Val()
			Expect(result[0].Values["b"]).To(Equal("2"))
			Expect(result[1].Values["c"]).To(Equal("3"))
		})

		It("XDEL fuzz test", func() {
			Expect(client.Del(ctx, "somestream").Err()).NotTo(HaveOccurred())
			var ids []string
			// add enough elements to have a few radix tree nodes inside the stream
			cnt := 0
			for {
				ids = append(ids, client.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: map[string]interface{}{"item": cnt}}).Val())
				cnt++
				if cnt > 500 {
					break
				}
			}
			// Now remove all the elements till we reach an empty stream and after every deletion,
			// check that the stream is sane enough to report the right number of elements with XRANGE:
			// this will also force accessing the whole data structure to check sanity.
			Expect(client.XLen(ctx, "somestream").Val()).To(Equal(int64(cnt)))
			// We want to remove elements in random order to really test the implementation in a better way.
			rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			for _, id := range ids {
				Expect(client.XDel(ctx, "somestream", id).Val()).To(Equal(int64(1)))
				cnt--
				Expect(client.XLen(ctx, "somestream").Val()).To(Equal(int64(cnt)))
				// The test would be too slow calling XRANGE for every iteration. Do it every 100 removal.
				if cnt%100 == 0 {
					items, err := client.XRange(ctx, "somestream", "-", "+").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(items).To(HaveLen(cnt))
				}
			}
		})

		It("XRANGE fuzzing", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			insertIntoStreamKey(client, "mystream")
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			lowID, highID := items[0].ID, items[len(items)-1].ID
			for i := 0; i < 100; i++ {
				start, end := streamRandomID(lowID, highID), streamRandomID(lowID, highID)
				realRange, err := client.XRange(ctx, "mystream", start, end).Result()
				Expect(err).NotTo(HaveOccurred())
				fakeRange := streamSimulateXRANGE(items, start, end)
				Expect(fakeRange).To(Equal(realRange))
			}
		})

		It("XREVRANGE regression test for (redis issue #5006)", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891240", Values: []string{"key2", "value2"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891250", Values: []string{"key3", "value3"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891240", Values: []string{"key1", "value2"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891250", Values: []string{"key1", "value3"}}).Err()).NotTo(HaveOccurred())
			items := client.XRevRange(ctx, "teststream", "1234567891245", "-").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0]).To(Equal(redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key2": "value2"}}))
			Expect(items[1]).To(Equal(redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}))
			items = client.XRevRange(ctx, "teststream2", "1234567891245", "-").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0]).To(Equal(redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key1": "value2"}}))
			Expect(items[1]).To(Equal(redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}))
		})

		It("XREAD streamID edge (no-blocking)", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-1", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-18446744073709551615", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-1", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred())
			r := client.XRead(ctx, &redis.XReadArgs{Streams: []string{"x", "1-18446744073709551615"}}).Val()
			Expect(r).To(HaveLen(1))
			Expect(r[0].Stream).To(Equal("x"))
			Expect(r[0].Messages).To(HaveLen(1))
			Expect(r[0].Messages[0].ID).To(Equal("2-1"))
			Expect(r[0].Messages[0].Values).To(Equal(map[string]interface{}{"f": "v"}))
		})

		It("XADD streamID edge", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2577343934890-18446744073709551615", Values: []string{"f", "v"}}).Err()).NotTo(HaveOccurred()) // we need the timestamp to be in the future
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", Values: []string{"f2", "v2"}}).Err()).NotTo(HaveOccurred())
			items := client.XRange(ctx, "x", "-", "+").Val()
			Expect(items).To(HaveLen(2))
			Expect(items[0]).To(Equal(redis.XMessage{ID: "2577343934890-18446744073709551615", Values: map[string]interface{}{"f": "v"}}))
			Expect(items[1]).To(Equal(redis.XMessage{ID: "2577343934891-0", Values: map[string]interface{}{"f2": "v2"}}))
		})

		It("XTRIM with MAXLEN option basic test", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				if rand.Float64() < 0.9 {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": i}}).Err()).NotTo(HaveOccurred())
				} else {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"yitem": i}}).Err()).NotTo(HaveOccurred())
				}
			}
			Expect(client.XTrimMaxLen(ctx, "mystream", 666).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(666)))
			Expect(client.XTrimMaxLen(ctx, "mystream", 555).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(555)))
		})

		It("XADD with LIMIT consecutive calls", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			for i := 0; i < 100; i++ {
				Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": "v"}}).Err()).NotTo(HaveOccurred())
			}
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(55)))
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(55)))
		})

		It("XADD advances the entries-added counter and sets the recorded-first-entry-id", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())

			reply, err := client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.EntriesAdded).To(Equal(int64(1)))
			Expect(reply.RecordedFirstEntryID).To(Equal("1-0"))

			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())
			reply, err = client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.EntriesAdded).To(Equal(int64(2)))
			Expect(reply.RecordedFirstEntryID).To(Equal("1-0"))
		})

		It("XDEL/TRIM are reflected by recorded first entry", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "3-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "4-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "5-0", Values: []string{"data", "a"}}).Err()).NotTo(HaveOccurred())

			reply, err := client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.EntriesAdded).To(Equal(int64(5)))
			Expect(reply.RecordedFirstEntryID).To(Equal("1-0"))

			Expect(client.XDel(ctx, "x", "2-0").Result()).To(Equal(int64(1)))
			reply, err = client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.RecordedFirstEntryID).To(Equal("1-0"))

			Expect(client.XDel(ctx, "x", "1-0").Result()).To(Equal(int64(1)))
			reply, err = client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.RecordedFirstEntryID).To(Equal("3-0"))

			Expect(client.XTrimMaxLen(ctx, "x", 2).Result()).To(Equal(int64(1)))
			reply, err = client.XInfoStreamFull(ctx, "x", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reply.RecordedFirstEntryID).To(Equal("4-0"))
		})

		It("Maximum XDEL ID behaves correctly", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())

			for i := 0; i < 3; i++ {
				Expect(client.XAdd(ctx, &redis.XAddArgs{
					Stream: "x",
					ID:     fmt.Sprintf("%d-0", i+1),
					Values: []string{"data", fmt.Sprintf("%c", 'a'+i)},
				}).Err()).NotTo(HaveOccurred())
			}

			r := client.XInfoStreamFull(ctx, "x", 0).Val()
			Expect(r.MaxDeletedEntryID).To(Equal("0-0"))

			Expect(client.XDel(ctx, "x", "2-0").Err()).NotTo(HaveOccurred())
			r = client.XInfoStreamFull(ctx, "x", 0).Val()
			Expect(r.MaxDeletedEntryID).To(Equal("2-0"))

			Expect(client.XDel(ctx, "x", "1-0").Err()).NotTo(HaveOccurred())
			r = client.XInfoStreamFull(ctx, "x", 0).Val()
			Expect(r.MaxDeletedEntryID).To(Equal("2-0"))
		})

		It("XADD can CREATE an empty stream", func() {
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())
			// Equivalent to TCL's `r XADD mystream MAXLEN 0 * a b`
			Expect(client.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				MaxLen: 0,
				Values: map[string]interface{}{"a": "b"},
			}).Err()).NotTo(HaveOccurred())

			// Equivalent to TCL's `[dict get [r xinfo stream mystream] length]`
			streamInfo, err := client.XInfoStream(ctx, "x").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(streamInfo.Length).To(Equal(int64(1)))
		})

		It("XADD with maximal seq", func() {
			// Equivalent to TCL's `r DEL x`
			Expect(client.Del(ctx, "x").Err()).NotTo(HaveOccurred())

			// Equivalent to TCL's `r XADD x 1-18446744073709551615 f1 v1`
			_, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     "1-18446744073709551615",
				Values: map[string]interface{}{"f1": "v1"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			// Equivalent to TCL's assert_error for `r XADD x 1-* f2 v2`
			_, err = client.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     "1-*",
				Values: map[string]interface{}{"f2": "v2"},
			}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("The ID specified in XADD is equal or smaller"))
		})

		It("XADD large data, triggering flushing, XREAD should work", func() {
			Expect(client.Do(ctx, "DEL", "flush_stream").Err()).NotTo(HaveOccurred())
			// 使用 xadd 插入两万条数据
			for i := 0; i < 20000; i++ {
				Expect(client.XAdd(ctx, &redis.XAddArgs{
					Stream: "flush_stream",
					ID:     "*",
					Values: []string{"item", strconv.Itoa(i)},
				}).Err()).NotTo(HaveOccurred())
			}

			lastID := "0-0"
			readCount := 0
			for {
				res, err := client.XRead(ctx, &redis.XReadArgs{
					Streams: []string{"flush_stream", lastID},
					Count:   1000,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				if len(res[0].Messages) == 0 {
					break
				}
				for _, xMessage := range res[0].Messages {
					readCount++
					lastID = xMessage.ID
				}
			}
			Expect(readCount).To(Equal(20000))
		})

		It("XADD large data, XLEN, XRANGE, XTRIM should work", func() {
			// 使用 xtrim 裁剪成只剩最新的100条数据
			Expect(client.XTrimMaxLen(ctx, "flush_stream", 100).Err()).NotTo(HaveOccurred())

			// 使用 xlen 检测
			length, err := client.XLen(ctx, "flush_stream").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(100)))

			// 使用 xrange 读取所有数据
			ranges, err := client.XRange(ctx, "flush_stream", "-", "+").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(ranges)).To(Equal(100))
			for i, xMessage := range ranges {
				Expect(xMessage.Values["item"]).To(Equal(strconv.Itoa(19900 + i)))
			}
			Expect(client.Do(ctx, "DEL", "flush_stream").Err()).NotTo(HaveOccurred())
		})
	})

	Describe("unpassed tests", func() {

	})
})
