package cosmosdb

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

type InsertionManagerMessage int

const (
	MsgSlowdown      InsertionManagerMessage = 1
	MsgSpeedup       InsertionManagerMessage = 2
	MsgRequestSample InsertionManagerMessage = 3
)

const (
	backupBufferSize              = 1000
	messageChannelSize            = 100
	workerScaleFactorWithIndex    = 1.45
	workerScaleFactorWithoutIndex = 2.00
	massHiringPercentage          = 0.7
	massHiringMaxWorkerPerHire    = 200
	massHiringSampleSize          = 5
)

type InsertionManager struct {
	latencyRecords     []float64
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
	dropIndex        bool
	ingestionChannel chan interface{}
}

func NewInsertionManager(ingestionChannel chan interface{}, collection *CosmosDBCollection, stopOnError bool, dropIndex bool) *InsertionManager {
	return &InsertionManager{
		latencyRecords:     make([]float64, 0, 0),
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
		dropIndex:        dropIndex,
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

//NotifyRateLimit is to be invoked from a worker to notify the manager to stop adding new workers
func (h *InsertionManager) notifyRateLimit() {
	atomic.AddUint64(&h.rateLimitCounter, 1)
}

// SubmitSample is invoked from a worker to provide the manager with information it needs to calculate whether we can go faster or not
func (h *InsertionManager) submitSample(workerID int, latency int64, charge int64) {
	if latency < 0 {
		return
	}
	defer h.recordWg.Done()
	h.latencyRecords[workerID] = float64(latency) / 1.0e+6
	h.consumptionRecords[workerID] = charge
}

// HireNewWorker launches a new worker routine to help with the ingestion work
func (h *InsertionManager) HireNewWorker() {
	session, err := h.SpecifySession()
	if err != nil {
		log.Logvf(log.Always, "Unable to obtain session for new worker: %v", err)
		return
	}
	collection := session.DB(h.collection.Collection.Database.Name).C(h.collection.Collection.Name)

	newWorkerID := h.workerCount
	worker := NewInsertionWorker(collection, newWorkerID, h.stopOnError)
	worker.NotifyOfStatistics = h.submitSample
	worker.NotifyOfThrottle = h.notifyRateLimit
	worker.OnDocumentIngestion = h.OnDocumentIngestion
	worker.ingestionChannel = h.ingestionChannel
	worker.backupChannel = h.backupChannel

	h.latencyRecords = append(h.latencyRecords, 0)
	h.consumptionRecords = append(h.consumptionRecords, 0)

	h.managerChannels = append(h.managerChannels, nil)
	h.managerChannels[newWorkerID] = make(chan InsertionManagerMessage, messageChannelSize)
	worker.messageChannel = h.managerChannels[newWorkerID]

	h.workerWg.Add(1)
	h.workerCount++

	go func() {
		defer session.Close()
		defer h.workerWg.Done()
		if err := worker.Run(); err != nil {
			log.Logvf(log.Always, "Worker %d exiting due to an error: %v", newWorkerID, err)
		}
	}()
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
	if h.dropIndex {
		if derr := h.collection.Collection.DropAllIndexes(); derr != nil {
			log.Logvf(log.Always, "Unable to drop index: %v", derr)
			return
		}
		log.Logv(log.Info, "Successfully DropAllIndex on Collection")
	}

	go func() {
		h.launchMassHiringManager()
		log.Logv(log.Info, "Mass hiring of workers is over; switching to single hires")
		h.launchSingleHiringManager()
	}()
}

func (h *InsertionManager) launchMassHiringManager() {
	estimateWorkerScaleFactor := workerScaleFactorWithIndex
	if h.dropIndex {
		estimateWorkerScaleFactor = workerScaleFactorWithoutIndex
	}
	log.Logvf(log.Info, "Estimate Worker Scale Factor: %.2f", estimateWorkerScaleFactor)

	for {
		sampleLatencyData := make([]float64, massHiringSampleSize)
		sampleChargeData := make([]float64, massHiringSampleSize)

		for i := 0; i < massHiringSampleSize; i++ {
			h.recordWg.Add(h.workerCount)
			for i := 0; i < h.workerCount; i++ {
				h.managerChannels[i] <- MsgRequestSample
			}
			h.recordWg.Wait()

			sampleLatencyData[i] = medianFloat64(h.latencyRecords)
			sampleChargeData[i] = meanInt64(h.consumptionRecords)

			time.Sleep(time.Second)
		}

		medianLatency := medianFloat64(sampleLatencyData)
		meanCharge := meanFloat64(sampleChargeData)
		log.Logvf(log.Info, "Median insertion every second took %.8f seconds and consumed %.2f RU", medianLatency, meanCharge)

		amount := int(math.Ceil((float64(h.collection.Throughput)*medianLatency)/meanCharge) * estimateWorkerScaleFactor)
		amountToHire := int(math.Ceil(float64(amount-h.workerCount) * massHiringPercentage))
		log.Logvf(log.Info, "Manager wants a total of workers %d and thus requests %d additional worker(s) to be hired.", amount, amountToHire)


		result, hiringErr := verifyMassHiringChoices(amountToHire, h.currentRateLimitCount(), h.slowDownCount)
		if hiringErr != nil {
			log.Logv(log.Info, "Manager failed to mass hire new workers because: "+hiringErr.Error())
			return
		}
		for i := 0; i < result; i++ {
			h.HireNewWorker()
		}
		log.Logvf(log.Info, "There are now a total of %d workers", h.workerCount)
	}
}

func (h *InsertionManager) launchSingleHiringManager() {
	for {
		time.Sleep(10 * time.Second)

		if rateLimitCount := h.currentRateLimitCount(); rateLimitCount > 0 {
			log.Logvf(log.Info, "There was %d `Request rate too large` responses, no extra workers are needed", rateLimitCount)

			if rateLimitCount > 0 {
				for i := uint64(0); i < uint64(math.Ceil(float64(rateLimitCount)/2.0)); i++ {
					h.signalSlowdown()
				}
			}
			time.Sleep(5 * time.Second)
			atomic.StoreUint64(&h.rateLimitCounter, 0)
			continue
		}

		if h.slowDownCount > 0 {
			h.signalSpeedup()
			log.Logv(log.Info, "Manager thinks we can move a bit faster; a worker can speed back up again")
		} else {
			h.HireNewWorker()
			log.Logvf(log.Info, "Manager thinks we can move a bit faster; there are now %d workers", h.workerCount)
		}
		time.Sleep(10 * time.Second)
		atomic.StoreUint64(&h.rateLimitCounter, 0)
	}
}

// currentRateLimitCount checks if any workers has reported being throttled since the last time the manager checked.
func (h *InsertionManager) currentRateLimitCount() uint64 {
	limitCount := atomic.LoadUint64(&h.rateLimitCounter)
	atomic.StoreUint64(&h.rateLimitCounter, 0)
	return limitCount
}

func (h *InsertionManager) signalSlowdown() {
	targetWorker := h.slowDownCount
	if targetWorker >= h.workerCount {
		targetWorker = targetWorker % h.workerCount
	}
	h.managerChannels[targetWorker] <- MsgSlowdown
	h.slowDownCount++
}

func (h *InsertionManager) signalSpeedup() {
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

func verifyMassHiringChoices(amountToHire int, rateLimitCount uint64, slowDownCount int) (int, error) {
	if rateLimitCount > 0 || slowDownCount > 0 {
		return 0, fmt.Errorf("manager was recently throttled")
	}
	if amountToHire <= 0 {
		return 0, fmt.Errorf("manager has an overflow of workers")
	}
	if amountToHire > massHiringMaxWorkerPerHire {
		log.Logvf(log.Info, "Manager capped out hiring at %d new workers", massHiringMaxWorkerPerHire)
		return massHiringMaxWorkerPerHire, nil
	}
	return amountToHire, nil
}
