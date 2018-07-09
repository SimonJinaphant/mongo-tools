package cosmosdb

import (
	"os"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/mongodb/mongo-tools/common/log"
)

// The CosmosDBCollectionInfo type holds metadata about a CosmosDB collection.
type CosmosDBCollectionInfo struct {
	ShardKey   string
	Throughput int
}

// CreateCustomCosmosDB explicitly creates the c collection for
// Azure CosmosDB, allowing the specification of throughput and shard keys
func CreateCustomCosmosDB(info CosmosDBCollectionInfo, c *mgo.Collection) error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"customAction", "CreateCollection"})
	cmd = append(cmd, bson.DocElem{"collection", c.Name})

	//TODO: Validate the throughput range for Fixed & Unlimited collections
	cmd = append(cmd, bson.DocElem{"offerThroughput", info.Throughput})

	//TODO: Validate the shardkey to be in an acceptable format
	if info.ShardKey != "" {
		cmd = append(cmd, bson.DocElem{"shardKey", info.ShardKey})
	}

	return c.Database.Run(cmd, nil)
}

// VerifyDocumentCount periodically sends a count operation until either the resulting count matches
// the expected count or the timeout occurs; this is essential for sharded Cosmos DB collection as there
// is a small chance the master does not have the latest count.
func VerifyDocumentCount(collection *mgo.Collection, expectedCount uint64) bool {
	countOpDeadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(countOpDeadline) {
			log.Logv(log.Always, "Time limit for counting has exceeded; some documents may have been lost during the restore")
			os.Exit(123)
		}

		currentCount, countErr := collection.Count()

		if countErr != nil {
			return false
		}

		if uint64(currentCount) != expectedCount {
			log.Logvf(log.Always, "CosmosDB only reported %v documents while we ingested %v documents, let's try counting again...", currentCount, expectedCount)
			time.Sleep(500 * time.Millisecond)
		} else {
			log.Logvf(log.Always, "%s has a total of %d documents in Azure Cosmos DB", collection.Name, currentCount)
			return true
		}
	}
}
