package pika_keys_analysis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBefore(t *testing.T) {
	err := Init("./cli/config.yaml")
	assert.Nil(t, err)
	for _, client := range PikaInstance.clients {
		result, err := client.FlushAll(context.Background()).Result()
		assert.Nil(t, err)
		t.Log(result)
	}
}

func TestPutKeyToPika(t *testing.T) {
	err := Init("./cli/config.yaml")
	assert.Nil(t, err)
	c1 := PikaInstance.clients[0]
	c2 := PikaInstance.clients[1]
	c3 := PikaInstance.clients[2]
	for i := 0; i < 5; i++ {
		key := "first_big_key_" + uuid.New().String()
		value := uuid.New().String()
		value += value
		value += value
		value += value
		value += value
		value += value
		c1.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 5; i++ {
		key := "second_big_key_" + uuid.New().String()
		value := uuid.New().String()
		value += value
		value += value
		value += value
		value += value
		c2.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 5; i++ {
		key := "third_big_key_" + uuid.New().String()
		value := uuid.New().String()
		value += value
		value += value
		value += value
		c3.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 5; i++ {
		key := "fourth_big_key_" + uuid.New().String()
		value := uuid.New().String()
		value += value
		c3.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 10000; i++ {
		key := "fifth_big_key_" + uuid.New().String()
		value := uuid.New().String()
		c1.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 10000; i++ {
		key := "fifth_big_key_" + uuid.New().String()
		value := uuid.New().String()
		c2.Set(context.Background(), key, value, 0)
	}
	for i := 0; i < 10000; i++ {
		key := "fifth_big_key_" + uuid.New().String()
		value := uuid.New().String()
		c3.Set(context.Background(), key, value, 0)
	}
	totalKeyNumber := PikaInstance.GetTotalKeyNumber()
	assert.Equal(t, 30020, totalKeyNumber)
}

func TestListBigKey(t *testing.T) {
	err := Init("./cli/config.yaml")
	assert.Nil(t, err)
	totalKeyNumber := PikaInstance.GetTotalKeyNumber()
	start := time.Now()
	bigKeys, err := PikaInstance.ListBigKeysByScan(context.Background())
	assert.Nil(t, err)
	for keyType, minHeap := range bigKeys {
		data := minHeap.GetTopN(Head)
		fmt.Printf("Type: %s, Head: %d\n", keyType, Head)
		if len(data) == 0 {
			fmt.Println("No big key found")
		}
		for _, v := range data {
			prefixKey := v.Key
			if len(prefixKey) > 20 {
				prefixKey = prefixKey[:20]
			}
			fmt.Printf("Key Prefix: %s, Size: %d, From: %s\n", prefixKey, v.UsedSize, v.Client)
		}
	}
	end := time.Now()
	t.Log("Total Key Number:", totalKeyNumber)
	t.Log("Cost Time:", end.Sub(start))
}

func TestCompressKeyAndDeCompress(t *testing.T) {
	err := Init("./cli/config.yaml")
	assert.Nil(t, err)
	key := "test_compress_key"
	value := uuid.New().String()
	value += value
	value += value
	value += value
	value += value
	PikaInstance.clients[0].Set(context.Background(), key, value, 0)
	// compress
	err = PikaInstance.CompressKey(context.Background(), key)
	assert.Nil(t, err)
	// get value after compress
	compressValue := PikaInstance.clients[0].Get(context.Background(), key).Val()
	// decompress when not save to pika
	decompressVal, err := PikaInstance.DecompressKey(context.Background(), key, false)
	assert.Nil(t, err)
	assert.Equal(t, value, decompressVal)
	notSaveVal := PikaInstance.clients[0].Get(context.Background(), key).Val()
	assert.Equal(t, compressValue, notSaveVal)
	// decompress when save to pika
	decompressVal, err = PikaInstance.DecompressKey(context.Background(), key, true)
	assert.Nil(t, err)
	saveVal := PikaInstance.clients[0].Get(context.Background(), key).Val()
	assert.Equal(t, value, decompressVal)
	assert.Equal(t, value, saveVal)
	// recover
	err = PikaInstance.RecoverKey(context.Background(), key, "recover_key")
	assert.Nil(t, err)
	recoverVal := PikaInstance.clients[0].Get(context.Background(), "recover_key").Val()
	assert.Equal(t, value, recoverVal)
}

func TestAfter(t *testing.T) {
	err := Init("./cli/config.yaml")
	assert.Nil(t, err)
	for _, client := range PikaInstance.clients {
		result, err := client.FlushAll(context.Background()).Result()
		assert.Nil(t, err)
		t.Log(result)
	}
}
