package topk

import (
	"math"

	"github.com/twmb/murmur3"
	"golang.org/x/exp/rand"

	"github.com/Khighness/gokit/heap"
)

// @Author KHighness
// @Update 2023-06-18

const DecayTableLen = 1 << 8

// HeavyKeeper algorithm structure.
//
// See: https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf
type HeavyKeeper struct {
	k           uint32
	width       uint32
	depth       uint32
	decay       float64
	lookupTable []float64
	minCount    uint32

	r        *rand.Rand
	buckets  [][]bucket
	minHeap  heap.MinHeap
	expelled chan Item
	total    uint64
}

// bucket structure.
type bucket struct {
	fingerprint uint32 // hash fingerprint
	count       uint32
}

// NewHeavyKeeper creates a new HeavyKeeper instance.
func NewHeavyKeeper(k, width, depth uint32, decay float64, minCount uint32) TopK {
	lookupTable := make([]float64, DecayTableLen)
	for i := 0; i < DecayTableLen; i++ {
		lookupTable[i] = math.Pow(decay, float64(i))
	}

	buckets := make([][]bucket, depth)
	for i := range buckets {
		buckets[i] = make([]bucket, width)
	}

	return &HeavyKeeper{
		k:           k,
		width:       width,
		depth:       depth,
		decay:       decay,
		lookupTable: lookupTable,
		minCount:    minCount,

		r:        rand.New(rand.NewSource(0)),
		buckets:  buckets,
		minHeap:  heap.NewMinHeap(k),
		expelled: make(chan Item, 32),
	}
}

func (hk *HeavyKeeper) Add(item string, incr uint32) (string, bool) {
	itemBytes := []byte(item)
	itemFingerprint := murmur3.Sum32(itemBytes)

	var maxCount uint32

	for i, row := range hk.buckets {
		bucketNo := murmur3.SeedSum32(uint32(i), itemBytes) % hk.width
		bucketFingerprint := row[bucketNo].fingerprint
		bucketCount := row[bucketNo].count

		if bucketCount == 0 { // The bucket is initial.
			row[bucketNo].fingerprint = itemFingerprint
			row[bucketNo].count = incr
			maxCount = max(maxCount, incr)

		} else if bucketFingerprint == itemFingerprint { // Fingerprints match, do increment.
			row[bucketNo].count += incr
			maxCount = max(maxCount, row[bucketNo].count)

		} else { // Fingerprints do not match, handle hash conflict.
			for localIncr := incr; localIncr > 0; localIncr-- {
				curCount := row[bucketCount].count

				var decay float64
				if row[bucketNo].count < DecayTableLen {
					decay = hk.lookupTable[curCount]
				} else {
					decay = hk.lookupTable[DecayTableLen-1]
				}

				if hk.r.Float64() < decay {
					row[bucketNo].count--
					if row[bucketNo].count == 0 {
						row[bucketNo].fingerprint = itemFingerprint
						row[bucketNo].count = localIncr
						maxCount = max(maxCount, localIncr)
						break
					}
				}
			}
		}
	}

	hk.total += uint64(incr)

	if maxCount < hk.minCount {
		return "", false
	}

	if hk.minHeap.IsFull() && maxCount < hk.minHeap.Min() {
		return "", false
	}

	itemHeapIdx, itemHeapExist := hk.minHeap.Find(item)
	if itemHeapExist {
		hk.minHeap.Fix(itemHeapIdx, maxCount)
		return "", true
	}

	expelled := hk.minHeap.Add(&heap.Node{Key: item, Val: maxCount})
	if expelled != nil {
		hk.expel(Item{Key: expelled.Key, Count: expelled.Val})
		return expelled.Key, true
	}

	return "", true
}

func (hk *HeavyKeeper) List() []Item {
	items := hk.minHeap.Sorted()
	result := make([]Item, 0, len(items))
	for _, item := range items {
		result = append(result, Item{Key: item.Key, Count: item.Val})
	}
	return result
}

func (hk *HeavyKeeper) Total() uint64 {
	return hk.total
}

func (hk *HeavyKeeper) Expelled() <-chan Item {
	return hk.expelled
}

func (hk *HeavyKeeper) Fading() {
	for _, row := range hk.buckets {
		for i := range row {
			row[i].count >>= 1
		}
	}
	hk.total >>= 1
	hk.minHeap.Halve()
}

func (hk *HeavyKeeper) expel(item Item) {
	select {
	case hk.expelled <- item:
	default:
	}
}

func max(a, b uint32) uint32 {
	if a < b {
		return b
	}
	return a
}
