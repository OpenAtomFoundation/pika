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
		for _, v := range data {
			fmt.Printf("key Prefix: %s, Size: %d, From: %s\n", v.Key[:10], v.UsedSize, v.Client)
		}
	}
	end := time.Now()
	t.Log("Total Key Number:", totalKeyNumber)
	t.Log("Cost Time:", end.Sub(start))
}
