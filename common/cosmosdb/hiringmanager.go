package cosmosdb

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
)

type HiringManager struct {
	latencyRecords     []int64
	consumptionRecords []int64

	rateLimitCounter int64
	workerCount      int
	throughput       int

	workerWg *sync.WaitGroup
	recordWg *sync.WaitGroup

	AddWorkerAction func(manager *HiringManager, workerId int) error
}

func NewHiringManager(defaultWorkers int, throughput int) *HiringManager {
	return &HiringManager{
		latencyRecords:     make([]int64, 0, defaultWorkers),
		consumptionRecords: make([]int64, 0, defaultWorkers),
		rateLimitCounter:   0,
		workerCount:        0,
		throughput:         throughput,
		workerWg:           new(sync.WaitGroup),
		recordWg:           new(sync.WaitGroup),
		AddWorkerAction:    nil,
	}
}

func (h *HiringManager) AwaitAllWorkers() {
	h.workerWg.Wait()
	log.Logv(log.Info, "All workers have finished")
}

func (h *HiringManager) CountWorkers() int {
	return h.workerCount
}

// CanNotify is invoked from the workers to check if it's allowed to notify new information
func (h *HiringManager) CanNotify(workerId int) bool {
	return atomic.LoadInt64(&h.latencyRecords[workerId]) == -1
}

// Notify is invoked from the workers to provide the manager with information it needs to calculate whether we can go faster or not
func (h *HiringManager) Notify(workerId int, latency int64, charge int64) {
	if latency < 0 {
		return
	}
	defer h.recordWg.Done()
	atomic.StoreInt64(&h.latencyRecords[workerId], latency)
	atomic.StoreInt64(&h.consumptionRecords[workerId], charge)
}

// Start launches the manager routine which periodically checks whether it can add new workers to speed up the ingestion task
func (h *HiringManager) Start(n int, disableWorkerScaling bool) {
	if h.AddWorkerAction == nil {
		log.Logv(log.Always, "Unable to start manager with no AddWorkerAction defined")
		return
	}
	for i := 0; i < n; i++ {
		h.HireNewWorker()
	}
	if disableWorkerScaling {
		log.Logv(log.Always, "Auto Scaling of Insertion Workers is disabled in this run")
		return
	}
	go func() {
		sleepTime := 5 * time.Second

		for {
			time.Sleep(sleepTime)

			h.recordWg.Add(h.workerCount)
			for i := 0; i < h.workerCount; i++ {
				atomic.StoreInt64(&h.latencyRecords[i], -1)
				atomic.StoreInt64(&h.consumptionRecords[i], -1)
			}
			h.recordWg.Wait()

			var latencySum int64
			var chargeSum int64
			for i := 0; i < h.workerCount; i++ {
				latencySum += atomic.LoadInt64(&h.latencyRecords[i])
				chargeSum += atomic.LoadInt64(&h.consumptionRecords[i])
			}
			averageLatency := latencySum / int64(h.workerCount)
			averageCharge := chargeSum / int64(h.workerCount)
			log.Logvf(log.Info, "On average, insertions took %d (ns) and consumed %d RU", averageLatency, averageCharge)

			amount := int(math.Ceil((float64(h.throughput) * float64(averageLatency) / 1000000.0) / float64(averageCharge)))
			amountToHire := (amount - h.workerCount) / 2
			log.Logvf(log.Info, "Manager wants a total of workers %d; thus an additional %d workers will be hired", amount, amountToHire)
			if amountToHire <= 0 || amountToHire > 100 || h.WasRecentlyRateLimited() {
				break
			}

			for i := 0; i < amountToHire; i++ {
				h.HireNewWorker()
			}
			log.Logvf(log.Info, "There are now a total of %d workers", h.workerCount)
			sleepTime = sleepTime + (3 * time.Second)
		}

		log.Logv(log.Info, "Hiring manager has stopped mass hiring; switching to single hires")

		for {
			// The Hiring manager is vulnerable to sudden changes, need further work on this.
			time.Sleep(10 * time.Second)

			if h.WasRecentlyRateLimited() {
				time.Sleep(30 * time.Second)
				continue
			}

			h.HireNewWorker()
			log.Logvf(log.Info, "Manager thinks we can move a bit faster; there are now %d workers", h.workerCount)
		}
	}()
}

// HireNewWorker launches a new parallel worker to help with the ingestion work
func (h *HiringManager) HireNewWorker() {
	h.latencyRecords = append(h.latencyRecords, 0)
	h.consumptionRecords = append(h.consumptionRecords, 0)
	newWorkerID := h.workerCount
	h.workerCount++

	h.workerWg.Add(1)
	go func() {
		defer h.workerWg.Done()
		if err := h.AddWorkerAction(h, newWorkerID); err != nil {
			log.Logvf(log.Always, "Worker %d exiting due to an error: %v", newWorkerID, err)
		}
	}()
}

//NotifyRateLimit is to be invoked from worker to notify the manager to stop adding new workers
func (h *HiringManager) NotifyRateLimit() {
	atomic.AddInt64(&h.rateLimitCounter, 1)
}

// WasRecentlyRateLimited is invoked from the manager to check if a worker has reported a RateLimit error since the last time it checked.
func (h *HiringManager) WasRecentlyRateLimited() bool {
	limitCount := atomic.LoadInt64(&h.rateLimitCounter)
	atomic.StoreInt64(&h.rateLimitCounter, 0)
	if limitCount > 0 {
		log.Logvf(log.Info, "There was %d `Request rate too large` responses, no extra workers are needed", limitCount)
		return true
	}
	return false
}
