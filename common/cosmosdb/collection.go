package cosmosdb

import (
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

	//TODO: Validate the throughput range for Fixed & Unlimited collections
	cmd = append(cmd, bson.DocElem{"offerThroughput", c.Throughput})

	//TODO: Validate the shardkey to be in an acceptable format
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
