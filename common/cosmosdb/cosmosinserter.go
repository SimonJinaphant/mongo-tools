package cosmosdb

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	CSV  = "csv"
	TSV  = "tsv"
	JSON = "json"
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
	insertOperation := mgo.CreateInsertOp(ci.collection.FullName, doc.(bson.D))
	opDeadline := time.Now().Add(5 * time.Second)

retry:
	latency, err := ci.collection.InsertWithOp(insertOperation)
	if err != nil {
		if qerr, ok := err.(*mgo.QueryError); ok {
			switch qerr.Code {

			// TooManyRequest
			case 16500:
				manager.NotifyRateLimit()
				//log.Logvf(log.Always, "We're overloading Cosmos DB; let's wait")
				time.Sleep(5 * time.Millisecond)

				if time.Now().After(opDeadline) {
					log.Logv(log.Always, "Maximum throughput retry exceeded 5 seconds; moving on")
				} else {
					goto retry
				}

			// Malformed Request
			case 9:
				log.Logv(log.Always, "The request sent was malformed")

			default:
				log.Logvf(log.Always, "Unknown QueryError code: %s", err)
			}
		} else {
			log.Logvf(log.Always, "Received something that is not a QueryError: %v", err)
		}
	} else {
		if manager.CanNotify(workerId) {
			insertCharge, _ := ci.collection.GetLastRequestStatistics()
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
