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
	collection           *mgo.Collection
	workerID             int
	waitTime             int
	stopOnError          bool
	shouldSendStatistics bool
	OnDocumentIngestion  func()
	NotifyOfStatistics   func(int, int64, int64)
	NotifyOfThrottle     func()
}

func NewInsertionWorker(collection *mgo.Collection, workerID int, stopOnError bool) *InsertionWorker {
	return &InsertionWorker{
		collection:           collection,
		workerID:             workerID,
		waitTime:             0,
		stopOnError:          stopOnError,
		shouldSendStatistics: false,
	}
}

func (iw *InsertionWorker) Run(ingestionChannel <-chan interface{}, backupChannel chan interface{},
	messageChannel <-chan InsertionManagerMessage) error {
	for {
		time.Sleep(time.Duration(iw.waitTime) * time.Millisecond)
		select {
		case managerMsg := <-messageChannel:
			switch managerMsg {
			case MsgSlowdown:
				iw.waitTime += awaitBetweenOpTime
				log.Logvf(log.Info, "Worker %d was told to slow down; it will now await %d ms between operations", iw.workerID, iw.waitTime)
			case MsgSpeedup:
				iw.waitTime -= awaitBetweenOpTime
				log.Logvf(log.Info, "Worker %d was told to speed back up; it will now await %d ms between operations", iw.workerID, iw.waitTime)
			case MsgRequestSample:
				iw.shouldSendStatistics = true
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
			document, alive := <-ingestionChannel
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
	tooManyRequestsDeadline := time.Now().Add(tooManyRequestTimeLimit * time.Second)

retry:
	latency, err := iw.collection.InsertWithOp(insertOperation)
	if err != nil {
		if qerr, ok := err.(*mgo.QueryError); ok {
			switch qerr.Code {

			case TooManyRequests:
				iw.NotifyOfThrottle()
				time.Sleep(5 * time.Millisecond)

				if time.Now().After(tooManyRequestsDeadline) {
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
		if iw.shouldSendStatistics {
			requestCost, err := iw.collection.GetLastRequestStatistics()
			if err != nil {
				log.Logv(log.Info, "Unable to get latest RU cost")
			}
			iw.NotifyOfStatistics(iw.workerID, latency, requestCost)
			iw.shouldSendStatistics = false
		}
	}
	return err
}
