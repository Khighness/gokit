package topk

import (
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

// @Author KHighness
// @Update 2023-06-18

func TestHeavyKeeper(t *testing.T) {
	zipF := rand.NewZipf(rand.New(rand.NewSource(uint64(time.Now().Unix()))), 3, 2, 1000)
	topK := NewHeavyKeeper(10, 10000, 5, 0, 0.925, false, 2, time.Second)
	dataMap := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := strconv.FormatUint(zipF.Uint64(), 10)
		dataMap[key] = dataMap[key] + 1
		topK.Add(key, 1)
	}
	var rate float64
	for _, node := range topK.List() {
		rate += math.Abs(float64(node.Count)-float64(dataMap[node.Key])) / float64(dataMap[node.Key])
		t.Logf("[TestHeavyKeeper] item %s, count %d, expect %d", node.Key, node.Count, dataMap[node.Key])
	}
	t.Logf("[TestHeavyKeeper] err rate avg: %f", rate)
	for i, node := range topK.List() {
		assert.Equal(t, strconv.FormatInt(int64(i), 10), node.Key)
		t.Logf("[TestHeavyKeeper] %s: %d", node.Key, node.Count)
	}
}

func TestHeavyKeeper_MultiGoroutine(t *testing.T) {
	topK := NewHeavyKeeper(5, 10000, 5, 0, 0.925, false, 2, time.Second)

	monitor := newFlowWorker(1*time.Second, func() {
		t.Logf("[TestHeavyKeeper_MultiGoroutine] topk: %+v", topK.List())
	})
	go monitor.work()

	total := 1000
	producers := make([]*flowWorker, total)
	for i := range producers {
		val := i
		producers[i] = newFlowWorker(time.Second, func() {
			topK.Add(strconv.Itoa(val), uint32(val*100))
		})
		go producers[i].work()
	}

	time.Sleep(20 * time.Second)

	for i, node := range topK.List() {
		assert.Equal(t, strconv.FormatInt(int64(total-i-1), 10), node.Key)
	}

	monitor.kill()
	for _, producer := range producers {
		producer.kill()
	}
}

func TestHeavyKeeper_BurstTraffic(t *testing.T) {
	topK := NewHeavyKeeper(5, 10000, 5, 0, 0.925, true, 2, 3*time.Second)

	monitor := newFlowWorker(1*time.Second, func() {
		t.Logf("[TestHeavyKeeper_MultiGoroutine] topk: %+v", topK.List())
	})
	go monitor.work()

	total := 100
	historyProducers := make([]*flowWorker, total)
	for i := range historyProducers {
		val := i
		historyProducers[i] = newFlowWorker(time.Second, func() {
			topK.Add(strconv.Itoa(val), uint32(val*100))
		})
		go historyProducers[i].work()
	}

	time.Sleep(10 * time.Second)

	burstProducer := newFlowWorker(time.Second, func() {
		topK.Add("BURST", uint32(1000_0000))
	})
	go burstProducer.work()

	time.Sleep(3 * time.Second)
	assert.Equal(t, "BURST", topK.List()[0].Key)

	monitor.kill()
	for _, producer := range historyProducers {
		producer.kill()
	}
	burstProducer.kill()
}

type flowInterface interface {
	isLive() bool
	work()
	kill()
}

type flowWorker struct {
	live     int32
	internal time.Duration
	logic    func()
	ticker   *time.Ticker
}

func newFlowWorker(internal time.Duration, logic func()) *flowWorker {
	return &flowWorker{
		live:     1,
		internal: internal,
		logic:    logic,
		ticker:   nil,
	}
}

func (w *flowWorker) isLive() bool {
	return atomic.LoadInt32(&w.live) == 1
}

func (w *flowWorker) kill() {
	atomic.StoreInt32(&w.live, 0)
	if w.ticker != nil {
		w.ticker.Stop()
	}
}

func (w *flowWorker) work() {
	w.ticker = time.NewTicker(w.internal)
	for w.isLive() {
		select {
		case <-w.ticker.C:
			w.logic()
		}
	}
}
