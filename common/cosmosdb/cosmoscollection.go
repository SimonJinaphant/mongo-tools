package cosmosdb

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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
