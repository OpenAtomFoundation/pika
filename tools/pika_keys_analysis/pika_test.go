package pika_keys_analysis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPutKeyToPika(t *testing.T) {
	err := Init("./cli/config.yaml")
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
	totalKeyNumber := PikaInstance.GetTotalKeyNumber()
	start := time.Now()
	bigKeys, err := PikaInstance.ListBigKeysByScan(context.Background())
	if err != nil {
		t.Error(err)
	}
	for keyType, minHeap := range bigKeys {
		data := minHeap.GetTopN(Head)
		fmt.Printf("Type: %s, Head: %d\n", keyType, Head)
		if len(data) == 0 {
			fmt.Println("No big key found")
		}
		for _, v := range data {
			prefixKey := v.Key
			if len(prefixKey) > 10 {
				prefixKey = prefixKey[:10]
			}
			fmt.Printf("Key Prefix: %s, Size: %d, From: %s\n", prefixKey, v.UsedSize, v.Client)
		}
	}
	end := time.Now()
	t.Log("Total Key Number:", totalKeyNumber)
	t.Log("Cost Time:", end.Sub(start))
}
