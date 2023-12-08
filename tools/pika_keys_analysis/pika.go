package pika_keys_analysis

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

// Pika contains all clients
type Pika struct {
	clients []*redis.Client
}

// NewPika create a new pika instance
func NewPika(configs []PikaConfig) *Pika {
	pika := &Pika{}
	for _, config := range configs {
		pika.clients = append(pika.clients, redis.NewClient(&redis.Options{
			Addr:     config.Addr,
			Password: config.Password,
			DB:       config.DB,
		}))
	}
	return pika
}

// SolveSingleClient solve single client
func SolveSingleClient(client *redis.Client, ctx context.Context, wg *sync.WaitGroup, ch chan map[string]*FixedSizeMinHeap) {
	defer wg.Done()
	localHeapMap := make(map[string]*FixedSizeMinHeap)
	for k := range Type {
		localHeapMap[Type[k]] = NewFixedSizeMinHeap()
	}
	var cursor int64 = 0
	goroutineCh := make(chan struct{}, GoroutineNum/len(PikaInstance.clients))
	wgClient := sync.WaitGroup{}
	startIterator := client.Scan(ctx, 0, "*", 1).Iterator()
	endKey := ""
	if startIterator.Next(ctx) {
		endKey = startIterator.Val()
	} else {
		fmt.Printf("Current client %s done, waiting for task to finish\n", client)
		return
	}
	start := false
	for {
		wgClient.Add(1)
		go func(curCursor int64) {
			defer wgClient.Done()
			goroutineCh <- struct{}{}
			defer func() {
				<-goroutineCh
			}()
			keys, _, err := client.Scan(ctx, uint64(curCursor), "*", int64(ScanSize)).Result()
			if err != nil {
				_ = fmt.Errorf("scan error: %s", err)
				return
			}
			for _, key := range keys {
				dataType, err := client.Type(ctx, key).Result()
				if err != nil {
					continue
				}
				if stringInSlice(dataType, Type) {
					var usedSize int64 = 0
					switch dataType {
					case "string":
						usedSize = client.StrLen(ctx, key).Val()
					case "hash":
						allMap := client.HGetAll(ctx, key).Val()
						for k, v := range allMap {
							usedSize += int64(len(k) + len(v))
						}
					case "list":
						list := client.LRange(ctx, key, 0, -1).Val()
						for _, v := range list {
							usedSize += int64(len(v))
						}
					case "set":
						set := client.SMembers(ctx, key).Val()
						for _, v := range set {
							usedSize += int64(len(v))
						}
					case "zset":
						zset := client.ZRangeWithScores(ctx, key, 0, -1).Val()
						for _, v := range zset {
							usedSize += int64(len(v.Member.(string)) + 8)
						}
					}
					localHeapMap[dataType].Add(KeyWithMemory{
						Key:      key,
						UsedSize: usedSize,
						Client:   client.String(),
					})
				}
			}
		}(cursor)
		keys, _, err := client.Scan(ctx, uint64(cursor), "*", 1).Result()
		if err != nil {
			_ = fmt.Errorf("scan error: %s", err)
			return
		}
		if start {
			if len(keys) == 0 || keys[0] == endKey {
				break
			}
		}
		cursor += int64(ScanSize)
		start = true
	}
	fmt.Printf("Current client %s done, waiting for task to finish\n", client)
	wgClient.Wait()
	ch <- localHeapMap
}

// ListBigKeysByScan list all keys in all clients
func (pika *Pika) ListBigKeysByScan(ctx context.Context) (map[string]*FixedSizeMinHeap, error) {
	result := make(map[string]*FixedSizeMinHeap)
	wg := sync.WaitGroup{}
	resultCh := make(chan map[string]*FixedSizeMinHeap, len(pika.clients))

	for _, client := range pika.clients {
		wg.Add(1)
		go func(c *redis.Client) {
			SolveSingleClient(c, ctx, &wg, resultCh)
		}(client)
	}

	wg.Wait()
	close(resultCh)

	for localHeapMap := range resultCh {
		for dataType, localHeap := range localHeapMap {
			if _, ok := result[dataType]; !ok {
				result[dataType] = NewFixedSizeMinHeap()
			}
			for _, keyWithMemory := range localHeap.data {
				result[dataType].Add(keyWithMemory)
			}
		}
	}

	return result, nil
}

func (pika *Pika) GetTotalKeyNumber() int {
	var totalKeyNumber = 0
	for _, client := range pika.clients {
		keyNumber := client.Keys(context.Background(), "*").Val()
		totalKeyNumber += len(keyNumber)
	}
	return totalKeyNumber
}

// stringInSlice Helper function to check if a string is in a slice of strings
func stringInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if strings.ToLower(s) == strings.ToLower(str) {
			return true
		}
	}
	return false
}
