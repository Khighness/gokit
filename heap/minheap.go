package heap

import (
	"fmt"
	"sort"
)

// @Author KHighness
// @Update 2023-06-18

// MinHeap structure.
type MinHeap struct {
	Nodes Nodes
	K     uint32
}

// NewMinHeap creates a MinHeap instance.
func NewMinHeap(k uint32) MinHeap {
	nodes := Nodes{}
	Init(&nodes)
	return MinHeap{
		Nodes: nodes,
		K:     k,
	}
}

// Add adds a node to the min heap and returns the expelled node if the heap is full.
func (h *MinHeap) Add(x *Node) *Node {
	if !h.IsFull() {
		Push(&h.Nodes, x)
	} else if x.Val > h.Min() {
		expelled := Pop(&h.Nodes)
		Push(&h.Nodes, x)
		return expelled.(*Node)
	}
	return nil
}

// Pop removes and returns the minimum node from the min heap.
func (h *MinHeap) Pop() *Node {
	if h.IsEmpty() {
		panic("MinHeap: heap is empty")
	}
	expelled := Pop(&h.Nodes)
	return expelled.(*Node)
}

// Fix re-establishes the min heap ordering after the element at index i has changed its value.
func (h *MinHeap) Fix(idx int, val uint32) {
	if idx < 0 || idx >= h.Len() {
		panic(fmt.Errorf("MinHeap: idx(%d) is out bound of [0, %d)", idx, h.Len()))
	}
	h.Nodes[idx].Val = val
	Fix(&h.Nodes, idx)
}

// Min returns the value of the minimum element.
func (h *MinHeap) Min() uint32 {
	if h.IsEmpty() {
		return 0
	}
	return h.Nodes[0].Val
}

// Find returns the value for the given key and if the key exists.
func (h *MinHeap) Find(key string) (int, bool) {
	for i := range h.Nodes {
		if h.Nodes[i].Key == key {
			return i, true
		}
	}
	return 0, false
}

// Sorted returns the Nodes sorted in descending order.
func (h *MinHeap) Sorted() Nodes {
	nodes := append([]*Node(nil), h.Nodes...)
	sort.Sort(sort.Reverse(Nodes(nodes)))
	return nodes
}

// Len returns the length of the min heap.
func (h *MinHeap) Len() int { return len(h.Nodes) }

// IsEmpty checks if the min heap is empty.
func (h *MinHeap) IsEmpty() bool { return h.Len() == 0 }

// IsFull checks if the min heap is full.
func (h *MinHeap) IsFull() bool { return uint32(h.Len()) >= h.K }

// Fade Fades the value of all nodes according to the specified factor.
func (h *MinHeap) Fade(factor uint32) {
	for _, node := range h.Nodes {
		node.Val /= factor
	}
}

// Node structure.
type Node struct {
	Key string
	Val uint32
}

// Nodes type.
type Nodes []*Node

func (n Nodes) Len() int { return len(n) }
func (n Nodes) Less(i, j int) bool {
	return (n[i].Val < n[j].Val) || (n[i].Val == n[j].Val && n[i].Key > n[j].Key)
}
func (n Nodes) Swap(i, j int)       { n[i], n[j] = n[j], n[i] }
func (n *Nodes) Push(x interface{}) { *n = append(*n, x.(*Node)) }
func (n *Nodes) Pop() interface{} {
	var node *Node
	node, *n = (*n)[len((*n))-1], (*n)[:len((*n))-1]
	return node
}
