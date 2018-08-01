package cosmosdb

import (
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
)

const (
	// 1 GB = 1024^3 as by JEDEC memory standards:
	gigabyte        = int64(1073741824)
	fileSizeWarning = int64(2 * gigabyte)
	fileSizeFailure = int64(4 * gigabyte)

	fileSizeWarningErrMsg = "File is larger than 2 GB"
	fileSizeFailureErrMsg = "File is larger than 4 GB"

	shardkeyMessage = `
	Re-run this tool with the parameters --shardKey <key>, where <key> is a valid Mongo DB Shard Key.
	For more information about Shard Key visit: https://docs.mongodb.com/manual/sharding/#shard-keys.`
)

func BenchmarkTime(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Logvf(log.Always, "%s took %s", name, elapsed)
}

func FilterStandardErrors(stopOnError bool, err error) error {
	if stopOnError || db.IsConnectionError(err) {
		return err
	}
	return nil
}

// VerifyDocumentCount periodically sends a count operation until either the resulting count matches
// the expected count or the timeout occurs; this is essential for sharded Cosmos DB collection as there
// is a small chance the master does not have the latest count.
func VerifyDocumentCount(collection *mgo.Collection, expectedCount uint64) error {
	countOpDeadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(countOpDeadline) {
			log.Logv(log.Always, "Time limit for counting has exceeded; some documents may have been lost during the ingestion into Cosmos DB")
			return fmt.Errorf("Time limit exceeded")
		}

		currentCount, countErr := collection.Count()

		if countErr != nil {
			return countErr
		}

		if uint64(currentCount) != expectedCount {
			log.Logvf(log.Always, "CosmosDB only reported %v documents while we ingested %v documents, let's try counting again...", currentCount, expectedCount)
			time.Sleep(500 * time.Millisecond)
		} else {
			log.Logvf(log.Always, "%s has a total of %d documents in Azure Cosmos DB", collection.Name, currentCount)
			return nil
		}
	}
}

func GetDocumentCount(collection *mgo.Collection) error {
	currentCount, countErr := collection.Count()
	if countErr != nil {
		return countErr
	}
	log.Logvf(log.Always, "%s has a total of %d documents in Azure Cosmos DB", collection.Name, currentCount)
	return nil
}

func ValidateSizeRequirement(shardKey string, fileSize int64, ignoreSizeWarning bool) error {
	log.Logvf(log.Info, "File size is: %d", fileSize)
	if shardKey == "" {
		if fileSize > fileSizeFailure {
			log.Logv(log.Always, "The file to be ingested is larger than 4GB; for performance reasons we require you specify a Shard Key when migrating into CosmosDB")
			log.Logv(log.Always, shardkeyMessage)
			return fmt.Errorf(fileSizeFailureErrMsg)
		}

		if ignoreSizeWarning {
			log.Logv(log.Always, "--ignoreSizeWarning is enabled, you may ingest into Azure Cosmos DB")
			return nil
		}

		if fileSize > fileSizeWarning {
			log.Logv(log.Always, "The file to be ingested is larger than 2GB; for best performance on Cosmos DB we recommend you specify a shard key")
			log.Logv(log.Always, shardkeyMessage)
			log.Logv(log.Always, "or suppress this warning with the flag --ignoreSizeWarning")
			return fmt.Errorf(fileSizeWarningErrMsg)
		}

		log.Logvf(log.Info, "The file to be ingested is under 2GB, which is acceptable for an Fixed (un-sharded) Cosmos DB collection")
	} else {
		log.Logvf(log.Info, "No need to check Cosmos DB ingestion size requirements since the Shard Key is defined")
	}
	return nil
}
