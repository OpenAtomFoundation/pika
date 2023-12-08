package pika_keys_analysis

import (
	"runtime"
	"sort"
	"sync"
)

var memStats runtime.MemStats

type KeyWithMemory struct {
	Key      string
	UsedSize int64
	Client   string
}

type FixedSizeMinHeap struct {
	data []KeyWithMemory
	mu   sync.Mutex
}

func NewFixedSizeMinHeap() *FixedSizeMinHeap {
	return &FixedSizeMinHeap{
		data: make([]KeyWithMemory, 0),
		mu:   sync.Mutex{},
	}
}

// Add element to heap
func (h *FixedSizeMinHeap) Add(value KeyWithMemory) {
	h.mu.Lock()
	defer h.mu.Unlock()
	runtime.ReadMemStats(&memStats)
	for memStats.Alloc > uint64(MemoryLimit) {
		h.Pop()
		runtime.ReadMemStats(&memStats)
	}
	h.Push(value)
}

// Len implements heap.Interface's Len method
func (h *FixedSizeMinHeap) Len() int {
	return len(h.data)
}

// Less implements heap.Interface's Less method
func (h *FixedSizeMinHeap) Less(i, j int) bool {
	return h.data[i].UsedSize < h.data[j].UsedSize
}

// Swap implements heap.Interface's Swap method
func (h *FixedSizeMinHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

// Push implements heap.Interface's Push method
func (h *FixedSizeMinHeap) Push(x interface{}) {
	h.data = append(h.data, x.(KeyWithMemory))
}

// Pop implements heap.Interface's Pop method
func (h *FixedSizeMinHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

// GetTopN get top n elements from heap
func (h *FixedSizeMinHeap) GetTopN(head int) []KeyWithMemory {
	sort.Slice(h.data, func(i, j int) bool {
		return h.data[i].UsedSize > h.data[j].UsedSize
	})
	if head > len(h.data) {
		head = len(h.data)
	}
	return h.data[:head]
}
