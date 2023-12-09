package pika_keys_analysis

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeap(t *testing.T) {
	keyList := make([]KeyWithMemory, 0)
	for i := 0; i < 100; i++ {
		keyList = append(keyList, KeyWithMemory{
			Key:      "test",
			UsedSize: int64(rand.Intn(2000)),
			Client:   strconv.Itoa(i),
		})
	}
	maxHeap := NewFixedSizeMinHeap()
	for i := 0; i < 100; i++ {
		maxHeap.Add(keyList[i])
	}
	// data from heap
	data := maxHeap.GetTopN(100)
	// data from sort
	sort.Slice(keyList, func(i, j int) bool {
		return keyList[i].UsedSize > keyList[j].UsedSize
	})
	// compare
	assert.ElementsMatch(t, data, keyList[:100])
	// print
	for i := 0; i < 100; i++ {
		fmt.Println(data[i].UsedSize)
	}
}
