package topk

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Khighness/gokit/heap"
	"github.com/twmb/murmur3"
	"golang.org/x/exp/rand"
)

// @Author KHighness
// @Update 2023-06-18

const DecayTableLen = 1 << 8

// HeavyKeeper algorithm structure.
//
// See: https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf
type HeavyKeeper struct {
	k        uint32
	width    uint32
	depth    uint32
	minCount uint32

	conflictDecayFactor float64
	conflictLookupTable []float64
	timeDecayTicker     *time.Ticker
	timeDecayEnable     int32
	timeDecayFactor     uint32

	r        *rand.Rand
	buckets  [][]bucket
	minHeap  heap.MinHeap
	expelled chan Item
	total    uint64

	mu *sync.RWMutex
}

// bucket structure.
type bucket struct {
	fingerprint uint32 // hash fingerprint
	count       uint32
}

// NewHeavyKeeper creates a new HeavyKeeper instance.
func NewHeavyKeeper(k, width, depth uint32, minCount uint32, conflictDecayFactor float64,
	timeDecayEnable bool, timeDecayFactor uint32, timeDecayInternal time.Duration) TopK {

	conflictLookupTable := make([]float64, DecayTableLen)
	for i := 0; i < DecayTableLen; i++ {
		conflictLookupTable[i] = math.Pow(conflictDecayFactor, float64(i))
	}
	buckets := make([][]bucket, depth)
	for i := range buckets {
		buckets[i] = make([]bucket, width)
	}

	heavyKeeper := &HeavyKeeper{
		k:        k,
		width:    width,
		depth:    depth,
		minCount: minCount,

		conflictDecayFactor: conflictDecayFactor,
		conflictLookupTable: conflictLookupTable,
		timeDecayTicker:     time.NewTicker(timeDecayInternal),
		timeDecayEnable:     boolToInt(timeDecayEnable),
		timeDecayFactor:     timeDecayFactor,

		r:        rand.New(rand.NewSource(0)),
		buckets:  buckets,
		minHeap:  heap.NewMinHeap(k),
		expelled: make(chan Item, 32),

		mu: &sync.RWMutex{},
	}

	go heavyKeeper.tick()

	return heavyKeeper
}

func (hk *HeavyKeeper) Add(item string, incr uint32) (string, bool) {
	hk.mu.Lock()
	defer hk.mu.Unlock()

	itemBytes := []byte(item)
	itemFingerprint := murmur3.Sum32(itemBytes)

	// Final count = max(row[bucketCount].count)
	var finalCount uint32

	// Iterate all the rows and process logic.
	for i, row := range hk.buckets {
		bucketNo := murmur3.SeedSum32(uint32(i), itemBytes) % hk.width
		bucketFingerprint := row[bucketNo].fingerprint
		bucketCount := row[bucketNo].count

		if bucketCount == 0 {
			// The bucket is initial.
			row[bucketNo].fingerprint = itemFingerprint
			row[bucketNo].count = incr
			finalCount = max(finalCount, incr)

		} else if bucketFingerprint == itemFingerprint {
			// Fingerprints match, do increment.
			row[bucketNo].count += incr
			finalCount = max(finalCount, row[bucketNo].count)

		} else {
			// Fingerprints do not match, handle hash conflict.
			for localIncr := incr; localIncr > 0; localIncr-- {
				decayFactor := hk.getDecayFactor(row[bucketNo].count)
				if hk.r.Float64() < decayFactor {
					row[bucketNo].count--
					if row[bucketNo].count == 0 {
						row[bucketNo].fingerprint = itemFingerprint
						row[bucketNo].count = localIncr
						finalCount = max(finalCount, localIncr)
						break
					}
				}
			}

		}
	}

	hk.total += uint64(incr)

	// Final count is less then the configured minimal count.
	if finalCount < hk.minCount {
		return "", false
	}

	// Final count cannot meet the condition to enter the min heap.
	if hk.minHeap.IsFull() && finalCount < hk.minHeap.Min() {
		return "", false
	}

	// Item is already in the min heap, adjust the min heap.
	itemHeapIdx, itemHeapExist := hk.minHeap.Find(item)
	if itemHeapExist {
		hk.minHeap.Fix(itemHeapIdx, finalCount)
		return "", true
	}

	// Add the new item into the min heap.
	expelled := hk.minHeap.Add(&heap.Node{Key: item, Val: finalCount})
	if expelled != nil {
		hk.expel(Item{Key: expelled.Key, Count: expelled.Val})
		return expelled.Key, true
	}

	return "", true
}

func (hk *HeavyKeeper) List() []Item {
	hk.mu.RLock()
	defer hk.mu.RUnlock()

	items := hk.minHeap.Sorted()
	result := make([]Item, 0, len(items))
	for _, item := range items {
		result = append(result, Item{Key: item.Key, Count: item.Val})
	}
	return result
}

func (hk *HeavyKeeper) Total() uint64 {
	hk.mu.RLock()
	defer hk.mu.RUnlock()

	return hk.total
}

func (hk *HeavyKeeper) Expelled() <-chan Item {
	return hk.expelled
}

func (hk *HeavyKeeper) Fading(enable bool) {
	atomic.StoreInt32(&hk.timeDecayEnable, boolToInt(enable))
}

func (hk *HeavyKeeper) tick() {
	for atomic.LoadInt32(&hk.timeDecayEnable) == 1 {
		select {
		case <-hk.timeDecayTicker.C:
			hk.executeFading()
		default:
		}
	}
}

func (hk *HeavyKeeper) executeFading() {
	hk.mu.Lock()
	defer hk.mu.Unlock()

	for _, row := range hk.buckets {
		for i := range row {
			row[i].count /= hk.timeDecayFactor
		}
	}
	hk.total /= uint64(hk.timeDecayFactor)
	hk.minHeap.Fading(hk.timeDecayFactor)
}

func (hk *HeavyKeeper) expel(item Item) {
	select {
	case hk.expelled <- item:
	default:
	}
}

func (hk *HeavyKeeper) getDecayFactor(count uint32) float64 {
	if count < DecayTableLen {
		return hk.conflictLookupTable[count]
	} else {
		return hk.conflictLookupTable[DecayTableLen-1]
	}
}

func max(a, b uint32) uint32 {
	if a < b {
		return b
	}
	return a
}

func boolToInt(ok bool) int32 {
	if ok {
		return 1
	}
	return 0
}
