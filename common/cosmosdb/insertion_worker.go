package cosmosdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	tooManyRequestTimeLimit = 5
	awaitBetweenOpTime      = 250
)

const (
	TooManyRequests  = 16500
	ServerOpTimeout  = 50
	MalformedRequest = 9
)

type InsertionWorker struct {
	collection          *mgo.Collection
	manager             *InsertionManager
	ingestionChannel    chan interface{}
	workerID            int
	stopOnError         bool
	shouldSample        bool
	OnDocumentIngestion func()
}

func NewInsertionWorker(collection *mgo.Collection, manager *InsertionManager,
	ingestionChannel chan interface{}, workerID int, stopOnError bool) *InsertionWorker {

	return &InsertionWorker{
		collection:       collection,
		manager:          manager,
		ingestionChannel: ingestionChannel,
		workerID:         workerID,
		stopOnError:      stopOnError,
		shouldSample:     false,
	}
}

func (iw *InsertionWorker) Run(messageChannel <-chan InsertionManagerMessage, backupChannel chan interface{}) error {
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
			case MsgRequestSample:
				iw.shouldSample = true
			default:
				log.Logvf(log.Info, "Worker %d got an unknown message from manager", iw.workerID)
			}

		case backupDoc := <-backupChannel:
			log.Logvf(log.Info, "Worker %d picked up a document from the backup channel", iw.workerID)
			if err := iw.insert(backupDoc); err != nil {
				if filterErr := FilterStandardErrors(iw.stopOnError, err); filterErr != nil {
					return filterErr
				}
				if strings.Contains(err.Error(), "duplicate key") {
					log.Logvf(log.Always, "Worker %d inserted a backup that seem to have previously succeeded", iw.workerID)
					continue
				}
				if strings.Contains(err.Error(), "ExceedInsertDeadline") {
					backupChannel <- backupDoc
					continue
				}
				log.Logvf(log.Always, "Worker %d failed to insert a backup document due to: %v", iw.workerID, err)
				return err
			}

		default:
			document, alive := <-iw.ingestionChannel
			if !alive {
				return nil
			}

			if err := iw.insert(document); err != nil {
				log.Logvf(log.Info, "Worker %d is backing up a document due to an error: %v", iw.workerID, err)
				backupChannel <- document

				if err = FilterStandardErrors(iw.stopOnError, err); err != nil {
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

retry:
	latency, err := iw.collection.InsertWithOp(insertOperation)
	if err != nil {
		if qerr, ok := err.(*mgo.QueryError); ok {
			switch qerr.Code {

			case TooManyRequests:
				iw.manager.NotifyRateLimit()
				time.Sleep(5 * time.Millisecond)

				if time.Now().After(TooManyRequestsDeadline) {
					return fmt.Errorf("ExceedInsertDeadline-Throughput")
				}
				goto retry

			case ServerOpTimeout:
				log.Logv(log.Always, "The server exceeded its alloted time limit to process this request")
				return fmt.Errorf("ExceedInsertDeadline-Timeout")

			case MalformedRequest:
				log.Logv(log.Always, "The request sent was malformed")

			default:
				log.Logvf(log.Always, "An unknown QuerryError occured: %d - %s", qerr.Code, err)
			}
		}
	} else {
		if iw.shouldSample {
			insertCharge, err := iw.collection.GetLastRequestStatistics()
			if err != nil {
				log.Logv(log.Info, "Unable to get RU cost from last operation")
			}
			iw.manager.SubmitSample(iw.workerID, latency, insertCharge)
			iw.shouldSample = false
		}
	}
	return err
}
