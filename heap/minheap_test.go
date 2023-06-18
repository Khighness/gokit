package heap

import (
	"strconv"
	"testing"
)

// @Author KHighness
// @Update 2023-06-18

func TestMinHeap_Add(t *testing.T) {
	h := NewMinHeap(3)
	for i := 0; i <= 100; i++ {
		h.Add(&Node{
			Key: strconv.Itoa(i),
			Val: uint32(i),
		})

		t.Logf("Add %d, Min: %d", i, h.Min())
	}

	t.Log(1 << 9)
}
