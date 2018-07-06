package cosmosdb

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	tooManyRequestTimeLimit = 5
	exceededTimeLimit       = 30
)

const (
	TooManyRequests   = 16500
	ExceededTimeLimit = 50
	MalformedRequest  = 9
)

type CosmosDbInserter struct {
	collection *mgo.Collection
}

func NewCosmosDbInserter(collection *mgo.Collection) *CosmosDbInserter {
	return &CosmosDbInserter{
		collection: collection,
	}
}

func (ci *CosmosDbInserter) Insert(doc interface{}, manager *HiringManager, workerId int) error {
	// Prevent the retry from re-creating the insertOp object again by explicitly storing it
	insertOperation := mgo.CreateInsertOp(ci.collection.FullName, doc)
	TooManyRequestsDeadline := time.Now().Add(tooManyRequestTimeLimit * time.Second)
	ExceededTimeLimitDeadline := time.Now().Add(exceededTimeLimit * time.Second)

retry:
	latency, err := ci.collection.InsertWithOp(insertOperation)
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
				manager.NotifyRateLimit()
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
		} else {
			log.Logvf(log.Always, "An error occured: %v", err)
		}
	} else {
		if manager.CanNotify(workerId) {
			insertCharge, err := ci.collection.GetLastRequestStatistics()
			if err != nil {
				log.Logv(log.Info, "Unable to get RU cost from last operation")
			}
			manager.Notify(workerId, latency, insertCharge)
		}
	}
	return err
}

// Flush is needed so that upserter implements flushInserter, but upserter
// doesn't buffer anything so we don't need to do anything in Flush.
func (ci *CosmosDbInserter) Flush() error {
	return nil
}
