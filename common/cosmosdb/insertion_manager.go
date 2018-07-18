package cosmosdb

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

type InsertionManagerMessage int

const (
	MsgSlowdown InsertionManagerMessage = 1
	MsgSpeedup  InsertionManagerMessage = 2
)

const (
	backupBufferSize          = 1000
	messageChannelSize        = 100
	estimateWorkerScaleFactor = 1.45
	massHiringPercentage      = 0.8
	massHiringMaxWorker       = 150
)

type InsertionManager struct {
	latencyRecords     []int64
	consumptionRecords []int64
	recordWg           *sync.WaitGroup

	workerCount int
	workerWg    *sync.WaitGroup

	collection *CosmosDBCollection

	managerChannels  []chan InsertionManagerMessage
	rateLimitCounter uint64
	slowDownCount    int

	backupChannel chan interface{}

	OnDocumentIngestion func()
	SpecifySession      func() (*mgo.Session, error)

	stopOnError      bool
	ingestionChannel chan interface{}
}

func NewInsertionManager(ingestionChannel chan interface{}, collection *CosmosDBCollection, stopOnError bool) *InsertionManager {
	return &InsertionManager{
		latencyRecords:     make([]int64, 0, 0),
		consumptionRecords: make([]int64, 0, 0),
		recordWg:           new(sync.WaitGroup),

		workerCount: 0,
		workerWg:    new(sync.WaitGroup),

		collection: collection,

		managerChannels:  make([]chan InsertionManagerMessage, 0),
		rateLimitCounter: 0,
		slowDownCount:    0,

		backupChannel: make(chan interface{}, backupBufferSize),

		OnDocumentIngestion: nil,
		SpecifySession:      nil,

		stopOnError:      stopOnError,
		ingestionChannel: ingestionChannel,
	}
}

func (h *InsertionManager) AwaitAllWorkers() {
	h.workerWg.Wait()
	log.Logv(log.Info, "All workers have finished")

	close(h.backupChannel)
	if len(h.backupChannel) != 0 {
		// TODO: Figure out what to do here... So far this part never seems to get invoke
		log.Logvf(log.Always, "%d document(s) previously failed to be restored.", len(h.backupChannel))
	}
}

func (h *InsertionManager) CountWorkers() int {
	return h.workerCount
}

// CanNotify is invoked from the workers to check if it's allowed to notify new information
func (h *InsertionManager) CanNotify(workerId int) bool {
	return atomic.LoadInt64(&h.latencyRecords[workerId]) == -1
}

// Notify is invoked from the workers to provide the manager with information it needs to calculate whether we can go faster or not
func (h *InsertionManager) Notify(workerId int, latency int64, charge int64) {
	if latency < 0 {
		return
	}
	defer h.recordWg.Done()
	atomic.StoreInt64(&h.latencyRecords[workerId], latency)
	atomic.StoreInt64(&h.consumptionRecords[workerId], charge)
}

// Start launches the manager routine which periodically checks whether it can add new workers to speed up the ingestion task
func (h *InsertionManager) Start(startingAmount int, disableWorkerScaling bool) {
	if h.SpecifySession == nil {
		log.Logv(log.Always, "Unable to start manager with no session defined")
		return
	}
	if startingAmount < 1 {
		log.Logv(log.Always, "Unable to start manager with a worker count less than 1")
		return
	}
	for i := 0; i < startingAmount; i++ {
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
			averageLatency := float64(latencySum) / float64(h.workerCount)
			averageCharge := float64(chargeSum) / float64(h.workerCount)
			log.Logvf(log.Info, "On average, insertions took %.2f nanoseconds and consumed %.2f RU", averageLatency, averageCharge)

			amount := int(math.Ceil((float64(h.collection.Throughput)*averageLatency/1.0e+6)/averageCharge) * estimateWorkerScaleFactor)
			amountToHire := int(math.Ceil(float64(amount-h.workerCount) * massHiringPercentage))
			log.Logvf(log.Info, "Manager wants a total of workers %d; thus an additional %d workers will be hired", amount, amountToHire)
			if amountToHire <= 0 || amountToHire > massHiringMaxWorker || h.CurrentRateLimitCount() > 0 || h.slowDownCount > 0 {
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

			if rateLimitCount := h.CurrentRateLimitCount(); rateLimitCount > 0 {
				log.Logvf(log.Info, "There was %d `Request rate too large` responses, no extra workers are needed", rateLimitCount)

				if rateLimitCount > 0 {
					for i := uint64(0); i < uint64(math.Ceil(float64(rateLimitCount)/2.0)); i++ {
						h.slowdownWorker()
					}
				}
				time.Sleep(5 * time.Second)
				atomic.StoreUint64(&h.rateLimitCounter, 0)
				continue
			}

			if h.slowDownCount > 0 {
				h.speedupWorker()
				log.Logv(log.Info, "Manager thinks we can move a bit faster; a worker can speed back up again")
			} else {
				h.HireNewWorker()
				log.Logvf(log.Info, "Manager thinks we can move a bit faster; there are now %d workers", h.workerCount)
			}
			time.Sleep(10 * time.Second)
			atomic.StoreUint64(&h.rateLimitCounter, 0)
		}
	}()
}

// HireNewWorker launches a new parallel worker to help with the ingestion work
func (h *InsertionManager) HireNewWorker() {
	session, err := h.SpecifySession()
	if err != nil {
		log.Logvf(log.Always, "Unable to obtain session for new worker: %v", err)
		return
	}
	collection := session.DB(h.collection.Collection.Database.Name).C(h.collection.Collection.Name)

	newWorkerID := h.workerCount
	worker := NewInsertionWorker(collection, h, h.ingestionChannel, newWorkerID, h.stopOnError)
	worker.OnDocumentIngestion = h.OnDocumentIngestion

	h.latencyRecords = append(h.latencyRecords, 0)
	h.consumptionRecords = append(h.consumptionRecords, 0)
	h.managerChannels = append(h.managerChannels, nil)
	h.managerChannels[newWorkerID] = make(chan InsertionManagerMessage, messageChannelSize)

	h.workerWg.Add(1)
	h.workerCount++

	go func() {
		defer session.Close()
		defer h.workerWg.Done()
		if err := worker.Run(h.managerChannels[newWorkerID], h.backupChannel); err != nil {
			log.Logvf(log.Always, "Worker %d exiting due to an error: %v", newWorkerID, err)
		}
	}()
}

//NotifyRateLimit is to be invoked from worker to notify the manager to stop adding new workers
func (h *InsertionManager) NotifyRateLimit() {
	atomic.AddUint64(&h.rateLimitCounter, 1)
}

// CurrentRateLimitCount is invoked from the manager to check if a worker has reported a RateLimit error since the last time it checked.
func (h *InsertionManager) CurrentRateLimitCount() uint64 {
	limitCount := atomic.LoadUint64(&h.rateLimitCounter)
	atomic.StoreUint64(&h.rateLimitCounter, 0)
	return limitCount
}

func (h *InsertionManager) slowdownWorker() {
	targetWorker := h.slowDownCount
	if targetWorker >= h.workerCount {
		targetWorker = targetWorker % h.workerCount
	}
	h.managerChannels[targetWorker] <- MsgSlowdown
	h.slowDownCount++
}

func (h *InsertionManager) speedupWorker() {
	if h.slowDownCount <= 0 {
		log.Logv(log.Info, "All workers are already running as fast as possible")
		return
	}

	targetWorker := h.slowDownCount - 1
	if targetWorker >= h.workerCount {
		targetWorker = targetWorker % h.workerCount
	}

	h.managerChannels[targetWorker] <- MsgSpeedup
	h.slowDownCount--
}
