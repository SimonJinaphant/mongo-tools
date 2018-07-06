package cosmosdb

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	opDeadlineMs              = 5000
	Error_RequestRateTooLarge = 16500
	Error_MalformedRequest    = 9
	Error_PartitionKey        = 2
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
	opDeadline := time.Now().Add(opDeadlineMs * time.Millisecond)

retry:
	latency, err := ci.collection.InsertWithOp(insertOperation)
	if err != nil {
		if qerr, ok := err.(*mgo.QueryError); ok {
			switch qerr.Code {
			case Error_PartitionKey:
				time.Sleep(50 * time.Millisecond)
				log.Logv(log.Always, "Partition key recovery...")
				fallthrough

			case Error_RequestRateTooLarge:
				manager.NotifyRateLimit()
				time.Sleep(5 * time.Millisecond)

				if time.Now().After(opDeadline) {
					log.Logv(log.Always, "Maximum throughput retry exceeded 5 seconds; moving on")
				} else {
					goto retry
				}

			case Error_MalformedRequest:
				log.Logv(log.Always, "The request sent was malformed")

			default:
				log.Logvf(log.Always, "Unknown QueryError code: %s", err)
			}
		} else {
			log.Logvf(log.Always, "Received something that is not a QueryError: %v", err)
		}
	} else {
		if manager.CanNotify(workerId) {
			insertCharge, err := ci.collection.GetLastRequestStatistics()
			if err != nil {
				log.Logv(log.Always, "Unable to get RU cost from last op")
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
