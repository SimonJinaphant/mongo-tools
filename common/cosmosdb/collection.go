package cosmosdb

import (
	"fmt"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	fileSizeWarning = int64(2e9)
	fileSizeFailure = int64(2 * fileSizeWarning)

	shardkeyMessage = `
	Re-run this tool with the parameters --shardKey <key>, where <key> is a valid Mongo DB Shard Key.
	For more information about Shard Key visit: https://docs.mongodb.com/manual/sharding/#shard-keys.`

	throughputMessage = `
	To ingest into Azure Cosmos DB you must specify a throughput via re-running the tool with --throughput <int>
	If you specified a Shard Key you can use a throughput value between 10,000 to 50,000;
	otherwise you can only use a value between 1 to 10,000.`
)

// The CosmosDBCollection type holds metadata about a CosmosDB collection.
type CosmosDBCollection struct {
	Collection *mgo.Collection
	ShardKey   string
	Throughput int
}

func NewCollection(collection *mgo.Collection, throughput int, shardKey string) *CosmosDBCollection {
	return &CosmosDBCollection{
		Collection: collection,
		Throughput: throughput,
		ShardKey:   shardKey,
	}
}

func (c *CosmosDBCollection) Deploy() error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"customAction", "CreateCollection"})
	cmd = append(cmd, bson.DocElem{"collection", c.Collection.Name})

	throughput := c.Throughput
	if throughput <= 0 {
		log.Logv(log.Always, throughputMessage)
		return fmt.Errorf("No valid throughput provided")
	}
	cmd = append(cmd, bson.DocElem{"offerThroughput", throughput})

	if c.ShardKey != "" {
		cmd = append(cmd, bson.DocElem{"shardKey", c.ShardKey})
	}

	err := c.Collection.Database.Run(cmd, nil)

	if err != nil {
		log.Logvf(log.Always, "Unable to create collection: %s", c.Collection.Name)
		log.Logv(log.Always, "If the collection already exist please re-run the tool with `--drop` to delete the pre-existing collection")
	}

	return err
}
