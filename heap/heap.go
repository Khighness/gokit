package heap

import "sort"

// @Author KHighness
// @Update 2023-06-18

// Heap interface.
type Heap interface {
	sort.Interface
	Push(x interface{})
	Pop() interface{}
}

// Init establishes the heap invariants.
func Init(h Heap) {
	n := h.Len()
	for i := (n >> 1) - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// Push pushes the element x onto the heap.
func Push(h Heap, x interface{}) {
	h.Push(x)
	up(h, h.Len()-1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
func Pop(h Heap) interface{} {
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.Pop()
}

// Remove removes and returns the element at index i from the heap.
func Remove(h Heap, i int) interface{} {
	n := h.Len() - 1
	if n != i {
		h.Swap(i, n)
		if !down(h, i, n) {
			up(h, i)
		}
	}
	return h.Pop()
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
func Fix(h Heap, i int) {
	if !down(h, i, h.Len()) {
		up(h, i)
	}
}

func up(h Heap, j int) {
	for {
		i := (j - 1) / 2 // parent node
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h Heap, i0, n int) bool {
	i := i0

	for {
		j1 := (i << 1) + 1 // left node
		if j1 >= n || j1 < 0 {
			break
		}

		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // right node
		}

		if !h.Less(j, i) {
			break
		}

		h.Swap(i, j)
		i = j
	}

	return i > i0
}
