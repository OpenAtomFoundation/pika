package pika_keys_analysis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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
