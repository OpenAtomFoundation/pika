package pika_keys_analysis

import (
	"container/heap"
	"runtime"
	"sync"
)

var memStats runtime.MemStats

type KeyWithMemory struct {
	Key      string
	UsedSize int64
	Client   string
}

type MaxHeap struct {
	data []KeyWithMemory
	mu   sync.Mutex
}

func NewFixedSizeMinHeap() *MaxHeap {
	return &MaxHeap{
		data: make([]KeyWithMemory, 0),
		mu:   sync.Mutex{},
	}
}

// Add element to heap
func (h *MaxHeap) Add(value KeyWithMemory) {
	h.mu.Lock()
	defer h.mu.Unlock()
	runtime.ReadMemStats(&memStats)
	for memStats.Alloc > uint64(MemoryLimit) {
		heap.Pop(h)
		runtime.ReadMemStats(&memStats)
	}
	heap.Push(h, value)
}

// Len implements heap.Interface's Len method
func (h *MaxHeap) Len() int {
	return len(h.data)
}

// Less implements heap.Interface's Less method
func (h *MaxHeap) Less(i, j int) bool {
	return h.data[i].UsedSize > h.data[j].UsedSize
}

// Swap implements heap.Interface's Swap method
func (h *MaxHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

// Push implements heap.Interface's Push method
func (h *MaxHeap) Push(x interface{}) {
	h.data = append(h.data, x.(KeyWithMemory))
}

// Pop implements heap.Interface's Pop method
func (h *MaxHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

// GetTopN get top n elements from heap
func (h *MaxHeap) GetTopN(head int) []KeyWithMemory {
	res := make([]KeyWithMemory, 0)
	for i := 0; i < head; i++ {
		if h.Len() == 0 {
			break
		}
		res = append(res, heap.Pop(h).(KeyWithMemory))
	}
	return res
}
