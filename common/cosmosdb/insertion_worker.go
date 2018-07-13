package cosmosdb

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	tooManyRequestTimeLimit = 5
	exceededTimeLimit       = 30
	awaitBetweenOpTime      = 500
)

const (
	TooManyRequests   = 16500
	ExceededTimeLimit = 50
	MalformedRequest  = 9
)

type InsertionWorker struct {
	collection          *mgo.Collection
	manager             *HiringManager
	ingestionChannel    chan interface{}
	workerID            int
	stopOnError         bool
	OnDocumentIngestion func()
}

func NewInsertionWorker(collection *mgo.Collection, manager *HiringManager,
	ingestionChannel chan interface{}, workerID int, stopOnError bool) *InsertionWorker {

	return &InsertionWorker{
		collection:       collection,
		manager:          manager,
		ingestionChannel: ingestionChannel,
		workerID:         workerID,
		stopOnError:      stopOnError,
	}
}

func (iw *InsertionWorker) Run(messageChannel <-chan HiringManagerMessage, backupChannel chan interface{}) error {
	waitTime := 0
	for {
		time.Sleep(time.Duration(waitTime) * time.Millisecond)
		select {
		case managerMsg := <-messageChannel:
			switch managerMsg {
			case MsgSlowdown:
				waitTime += awaitBetweenOpTime
				log.Logvf(log.Info, "Worker %d was told to slow down; it will now await %d ms between operations", iw.workerID, waitTime)
			case MsgSpeedup:
				waitTime -= awaitBetweenOpTime
				log.Logvf(log.Info, "Worker %d was told to speed back up; it will now await %d ms between operations", iw.workerID, waitTime)
			default:
				log.Logvf(log.Info, "Worker %d got an unknown message from manager", iw.workerID)
			}

		case backupDoc := <-backupChannel:
			log.Logvf(log.Info, "Worker %d picked up a document from the backup channel", iw.workerID)
			if err := iw.insert(backupDoc); err != nil {
				if err = FilterUnrecoverableErrors(iw.stopOnError, err); err != nil {
					return err
				}
			}

		default:
			document, alive := <-iw.ingestionChannel
			if !alive {
				return nil
			}

			if err := iw.insert(document); err != nil {
				log.Logvf(log.Info, "Worker %d is backing up a document due to an error: %v", iw.workerID, err)
				backupChannel <- document

				if err = FilterUnrecoverableErrors(iw.stopOnError, err); err != nil {
					return err
				}

				log.Logvf(log.Info, "Worker %d is able to recover from the error and go back in action", iw.workerID)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		if iw.OnDocumentIngestion != nil {
			iw.OnDocumentIngestion()
		}
	}
}

func (iw *InsertionWorker) insert(doc interface{}) error {
	// Prevent the retry from re-creating the insertOp object again by explicitly storing it
	insertOperation := mgo.CreateInsertOp(iw.collection.FullName, doc)
	TooManyRequestsDeadline := time.Now().Add(tooManyRequestTimeLimit * time.Second)
	ExceededTimeLimitDeadline := time.Now().Add(exceededTimeLimit * time.Second)

retry:
	latency, err := iw.collection.InsertWithOp(insertOperation)
	if err != nil {
		if qerr, ok := err.(*mgo.QueryError); ok {
			switch qerr.Code {
			case ExceededTimeLimit:
				log.Logv(log.Always, "Requests are exceeding time limit...let's take a 10 second break")
				time.Sleep(10 * time.Second)

				if time.Now().After(ExceededTimeLimitDeadline) {
					log.Logvf(log.Always, "Maximum retries for `Exceeded Time Limit` exceeded %d seconds; moving on", exceededTimeLimit)
				} else {
					goto retry
				}

			case TooManyRequests:
				iw.manager.NotifyRateLimit()
				time.Sleep(5 * time.Millisecond)

				if time.Now().After(TooManyRequestsDeadline) {
					log.Logvf(log.Always, "Maximum retries for `Throughput` exceeded %d seconds; moving on", tooManyRequestTimeLimit)
				} else {
					goto retry
				}

			case MalformedRequest:
				log.Logv(log.Always, "The request sent was malformed")

			default:
				log.Logvf(log.Always, "An unknown QuerryError occured: %d - %s", qerr.Code, err)
			}
		}
	} else {
		if iw.manager.CanNotify(iw.workerID) {
			insertCharge, err := iw.collection.GetLastRequestStatistics()
			if err != nil {
				log.Logv(log.Info, "Unable to get RU cost from last operation")
			}
			iw.manager.Notify(iw.workerID, latency, insertCharge)
		}
	}
	return err
}
