package heap

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

// @Author KHighness
// @Update 2023-06-18

func TestMinHeap_Api(t *testing.T) {
	k := 3
	l := 100
	h := initMinHeap(uint32(k))

	for i := k; i <= l; i++ {
		h.Add(&Node{
			Key: strconv.Itoa(i),
			Val: uint32(i),
		})

		assert.Equal(t, uint32(i-2), h.Min())
		assert.Equal(t, k, h.Len())
	}

	for i := k; i > 0; i-- {
		assert.Equal(t, h.Pop().Val, uint32(l-i+1))
	}
}

func initMinHeap(k uint32) MinHeap {
	h := NewMinHeap(k)
	for i := 0; i < int(k); i++ {
		h.Add(&Node{
			Key: strconv.Itoa(i),
			Val: uint32(i),
		})
	}
	return h
}
