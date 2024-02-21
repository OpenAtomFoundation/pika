package pika_keys_analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
func SolveSingleClient(client *redis.Client, ctx context.Context, wg *sync.WaitGroup, ch chan map[string]*MaxHeap) {
	defer wg.Done()
	localHeapMap := make(map[string]*MaxHeap)
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
		cursor += int64(ScanSize)
		start = true
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
	}
	fmt.Printf("Current client %s done, waiting for task to finish\n", client)
	wgClient.Wait()
	ch <- localHeapMap
}

// ListBigKeysByScan list all keys in all clients
func (pika *Pika) ListBigKeysByScan(ctx context.Context) (map[string]*MaxHeap, error) {
	result := make(map[string]*MaxHeap)
	wg := sync.WaitGroup{}
	resultCh := make(chan map[string]*MaxHeap, len(pika.clients))

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

func (pika *Pika) CompressKey(context context.Context, key string) error {
	for _, client := range pika.clients {
		exist := client.Exists(context, key).Val()
		if exist == 0 {
			continue
		}
		dataType := client.Type(context, key).Val()
		switch dataType {
		case "string":
			val := client.Get(context, key).Val()
			err := saveLocal([]byte(key), []byte(val))
			if err != nil {
				fmt.Printf("Current client %s cannot save %s to local: %s\n", client, key, err)
				continue
			}
			compressedVal, err := compress([]byte(val))
			if err != nil {
				fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
				continue
			}
			_, err = client.Set(context, key, compressedVal, 0).Result()
			if err != nil {
				fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
				continue
			}
			fmt.Printf("Original value: %s\n", val)
			fmt.Printf("Compressed value: %s\n", compressedVal)
			fmt.Printf("Current client %s compress key %s success\n", client, key)
		case "hash":
			val := client.HGetAll(context, key).Val()
			err := saveLocal([]byte(key), []byte(fmt.Sprintf("%v", val)))
			if err != nil {
				fmt.Printf("Current client %s cannot save %s to local: %s\n", client, key, err)
				continue
			}
			for k, v := range val {
				compressedVal, err := compress([]byte(v))
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				_, err = client.HSet(context, key, k, compressedVal).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				fmt.Printf("Original value: %s\n", val)
				fmt.Printf("Compressed value: %s\n", compressedVal)
			}
			fmt.Printf("Current client %s compress key %s success\n", client, key)
		case "list":
			val := client.LRange(context, key, 0, -1).Val()
			err := saveLocal([]byte(key), []byte(fmt.Sprintf("%v", val)))
			if err != nil {
				fmt.Printf("Current client %s cannot save %s to local: %s\n", client, key, err)
				continue
			}
			for _, v := range val {
				compressedVal, err := compress([]byte(v))
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				_, err = client.LPush(context, key, compressedVal).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				// delete the original value
				_, err = client.LRem(context, key, 1, v).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				fmt.Printf("Original value: %s\n", val)
				fmt.Printf("Compressed value: %s\n", compressedVal)
			}
			fmt.Printf("Current client %s compress key %s success\n", client, key)
		case "set":
			val := client.SMembers(context, key).Val()
			err := saveLocal([]byte(key), []byte(fmt.Sprintf("%v", val)))
			if err != nil {
				fmt.Printf("Current client %s cannot save %s to local: %s\n", client, key, err)
				continue
			}
			for _, v := range val {
				compressedVal, err := compress([]byte(v))
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				_, err = client.SAdd(context, key, compressedVal).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				// delete the original value
				_, err = client.SRem(context, key, v).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				fmt.Printf("Original value: %s\n", val)
				fmt.Printf("Compressed value: %s\n", compressedVal)
			}
			fmt.Printf("Current client %s compress key %s success\n", client, key)
		case "zset":
			val := client.ZRangeWithScores(context, key, 0, -1).Val()
			err := saveLocal([]byte(key), []byte(fmt.Sprintf("%v", val)))
			if err != nil {
				fmt.Printf("Current client %s cannot save %s to local: %s\n", client, key, err)
				continue
			}
			for _, v := range val {
				compressedVal, err := compress([]byte(v.Member.(string)))
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				_, err = client.ZAdd(context, key, &redis.Z{
					Score:  v.Score,
					Member: compressedVal,
				}).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				// delete the original value
				_, err = client.ZRem(context, key, v.Member.(string)).Result()
				if err != nil {
					fmt.Printf("Current client %s compress key %s error: %s\n", client, key, err)
					continue
				}
				fmt.Printf("Original value: %v\n", val)
				fmt.Printf("Compressed value: %s\n", compressedVal)
			}
			fmt.Printf("Current client %s compress key %s success\n", client, key)
		}
	}
	return nil
}

func (pika *Pika) DecompressKey(context context.Context, key string, save bool) (interface{}, error) {
	for _, client := range pika.clients {
		exist := client.Exists(context, key).Val()
		if exist == 0 {
			continue
		}
		dataType := client.Type(context, key).Val()
		switch dataType {
		case "string":
			val := client.Get(context, key).Val()
			decompressedVal, err := decompress([]byte(val))
			if err != nil {
				// maybe not compress, just continue
				continue
			}
			if save {
				_, err := client.Set(context, key, string(decompressedVal), 0).Result()
				if err != nil {
					fmt.Printf("Current client %s decompress key %s error: %s\n", client, key, err)
					continue
				}
			}
			return string(decompressedVal), nil
		case "hash":
			val := client.HGetAll(context, key).Val()
			type Hash struct {
				Field string
				Value string
			}
			var hash []Hash
			for k, v := range val {
				decompressedVal, err := decompress([]byte(v))
				if err != nil {
					// maybe not compress, just continue
					continue
				}
				if save {
					_, err := client.HSet(context, key, k, string(decompressedVal)).Result()
					if err != nil {
						fmt.Printf("Current client %s decompress key %s error: %s\n", client, key, err)
						continue
					}
				}
				hash = append(hash, Hash{
					Field: k,
					Value: string(decompressedVal),
				})
			}
			return hash, nil
		case "list":
			val := client.LRange(context, key, 0, -1).Val()
			list := make([]string, 0)
			for _, v := range val {
				decompressedVal, err := decompress([]byte(v))
				if err != nil {
					// maybe not compress, just continue
					continue
				}
				if save {
					_, err := client.LPush(context, key, string(decompressedVal)).Result()
					if err != nil {
						fmt.Printf("Current client %s decompress key %s error: %s\n", client, key, err)
						continue
					}
				}
				list = append(list, string(decompressedVal))
			}
			return list, nil
		case "set":
			val := client.SMembers(context, key).Val()
			set := make([]string, 0)
			for _, v := range val {
				decompressedVal, err := decompress([]byte(v))
				if err != nil {
					continue
				}
				if save {
					_, err := client.SAdd(context, key, string(decompressedVal)).Result()
					if err != nil {
						fmt.Printf("Current client %s decompress key %s error: %s\n", client, key, err)
						continue
					}
				}
				set = append(set, string(decompressedVal))
			}
			return set, nil
		case "zset":
			val := client.ZRangeWithScores(context, key, 0, -1).Val()
			zset := make([]redis.Z, 0)
			for _, v := range val {
				decompressedVal, err := decompress([]byte(v.Member.(string)))
				if err != nil {
					continue
				}
				if save {
					_, err := client.ZAdd(context, key, &redis.Z{
						Score:  v.Score,
						Member: string(decompressedVal),
					}).Result()
					if err != nil {
						fmt.Printf("Current client %s decompress key %s error: %s\n", client, key, err)
						continue
					}
				}
				zset = append(zset, redis.Z{
					Score:  v.Score,
					Member: string(decompressedVal),
				})
			}
			return zset, nil
		}
	}
	return nil, fmt.Errorf("key %s not found or has been decompressed", key)
}

func (pika *Pika) RecoverKey(context context.Context, from string, to string) error {
	file, err := os.ReadFile(filepath.Join(Save, from))
	if err != nil {
		return err
	}
	for _, client := range pika.clients {
		exist := client.Exists(context, from).Val()
		if exist == 0 {
			continue
		}
		dataType := client.Type(context, from).Val()
		switch dataType {
		case "string":
			_, err := client.Set(context, to, string(file), 0).Result()
			if err != nil {
				fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
				continue
			}
			fmt.Printf("New Key: %s, Value: %s\n", to, string(file))
			return nil
		case "hash":
			var hash []map[string]string
			err := json.Unmarshal(file, &hash)
			if err != nil {
				fmt.Printf("Current client %s recover key %s error: %s\n", client, from, err)
				continue
			}
			for _, v := range hash {
				for k, v := range v {
					_, err := client.HSet(context, to, k, v).Result()
					if err != nil {
						fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
						continue
					}
				}
			}
			fmt.Printf("New Key: %s, Value: %s\n", to, string(file))
			return nil
		case "list":
			var list []string
			err := json.Unmarshal(file, &list)
			if err != nil {
				fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
				continue
			}
			for _, v := range list {
				_, err := client.LPush(context, to, v).Result()
				if err != nil {
					fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
					continue
				}
			}
			fmt.Printf("New Key: %s, Value: %s\n", to, string(file))
			return nil
		case "set":
			var set []string
			err := json.Unmarshal(file, &set)
			if err != nil {
				fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
				continue
			}
			for _, v := range set {
				_, err := client.SAdd(context, to, v).Result()
				if err != nil {
					fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
					continue
				}
			}
			fmt.Printf("New Key: %s, Value: %s\n", to, string(file))
			return nil
		case "zset":
			var zset []redis.Z
			err := json.Unmarshal(file, &zset)
			if err != nil {
				fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
				continue
			}
			for _, v := range zset {
				_, err := client.ZAdd(context, to, &redis.Z{
					Score:  v.Score,
					Member: v.Member,
				}).Result()
				if err != nil {
					fmt.Printf("Current client %s recover key %s error: %s\n", client, to, err)
					continue
				}
			}
			fmt.Printf("New Key: %s, Value: %s\n", to, string(file))
			return nil
		}
	}
	return fmt.Errorf("key %s not found", from)
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
