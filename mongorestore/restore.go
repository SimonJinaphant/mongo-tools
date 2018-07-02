// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/mongodb/mongo-tools/common/cosmosdb"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/intents"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/util"
)

const insertBufferFactor = 16

// RestoreIntents iterates through all of the intents stored in the IntentManager, and restores them.
func (restore *MongoRestore) RestoreIntents() error {
	log.Logvf(log.DebugLow, "restoring up to %v collections in parallel", restore.OutputOptions.NumParallelCollections)

	if restore.OutputOptions.NumParallelCollections > 0 {
		resultChan := make(chan error)

		// start a goroutine for each job thread
		for i := 0; i < restore.OutputOptions.NumParallelCollections; i++ {
			go func(id int) {
				log.Logvf(log.DebugHigh, "starting restore routine with id=%v", id)
				var ioBuf []byte
				for {
					intent := restore.manager.Pop()
					if intent == nil {
						log.Logvf(log.DebugHigh, "ending restore routine with id=%v, no more work to do", id)
						resultChan <- nil // done
						return
					}
					if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
						if ioBuf == nil {
							ioBuf = make([]byte, db.MaxBSONSize)
						}
						fileNeedsIOBuffer.TakeIOBuffer(ioBuf)
					}
					err := restore.RestoreIntent(intent)
					if err != nil {
						resultChan <- fmt.Errorf("%v: %v", intent.Namespace(), err)
						return
					}
					restore.manager.Finish(intent)
					if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
						fileNeedsIOBuffer.ReleaseIOBuffer()
					}

				}
			}(i)
		}

		// wait until all goroutines are done or one of them errors out
		for i := 0; i < restore.OutputOptions.NumParallelCollections; i++ {
			if err := <-resultChan; err != nil {
				return err
			}
		}
		return nil
	}

	// single-threaded
	for {
		intent := restore.manager.Pop()
		if intent == nil {
			break
		}
		err := restore.RestoreIntent(intent)
		if err != nil {
			return fmt.Errorf("%v: %v", intent.Namespace(), err)
		}
		restore.manager.Finish(intent)
	}
	return nil
}

// RestoreIntent attempts to restore a given intent into MongoDB.
func (restore *MongoRestore) RestoreIntent(intent *intents.Intent) error {

	collectionExists, err := restore.CollectionExists(intent)
	if err != nil {
		return fmt.Errorf("error reading database: %v", err)
	}

	if restore.safety == nil && !restore.OutputOptions.Drop && collectionExists {
		log.Logvf(log.Always, "restoring to existing collection %v without dropping", intent.Namespace())
		log.Logv(log.Always, "Important: restored data will be inserted without raising errors; check your server log")
	}

	if restore.OutputOptions.Drop {
		if collectionExists {
			if strings.HasPrefix(intent.C, "system.") {
				log.Logvf(log.Always, "cannot drop system collection %v, skipping", intent.Namespace())
			} else {
				log.Logvf(log.Info, "dropping collection %v before restoring", intent.Namespace())
				err = restore.DropCollection(intent)
				if err != nil {
					return err // no context needed
				}
				log.Logv(log.Always, "Collection dropped!, let's wait 5 seconds to avoid caching issues")
				time.Sleep(5 * time.Second)
				collectionExists = false
			}
		} else {
			log.Logvf(log.DebugLow, "collection %v doesn't exist, skipping drop command", intent.Namespace())
		}
	}

	var options bson.D
	var indexes []IndexDocument
	var uuid string

	// get indexes from system.indexes dump if we have it but don't have metadata files
	if intent.MetadataFile == nil {
		if _, ok := restore.dbCollectionIndexes[intent.DB]; ok {
			if indexes, ok = restore.dbCollectionIndexes[intent.DB][intent.C]; ok {
				log.Logvf(log.Always, "no metadata; falling back to system.indexes")
			}
		}
	}

	logMessageSuffix := "with no metadata"
	// first create the collection with options from the metadata file
	if intent.MetadataFile != nil {
		logMessageSuffix = "using options from metadata"
		err = intent.MetadataFile.Open()
		if err != nil {
			return err
		}
		defer intent.MetadataFile.Close()

		log.Logvf(log.Always, "reading metadata for %v from %v", intent.Namespace(), intent.MetadataLocation)
		metadataJSON, err := ioutil.ReadAll(intent.MetadataFile)
		if err != nil {
			return fmt.Errorf("error reading metadata from %v: %v", intent.MetadataLocation, err)
		}
		metadata, err := restore.MetadataFromJSON(metadataJSON)
		if err != nil {
			return fmt.Errorf("error parsing metadata from %v: %v", intent.MetadataLocation, err)
		}
		if metadata != nil {
			options = metadata.Options
			indexes = metadata.Indexes
			if restore.OutputOptions.PreserveUUID {
				if metadata.UUID == "" {
					return fmt.Errorf("--preserveUUID used but no UUID found in %v", intent.MetadataLocation)
				}
				uuid = metadata.UUID
			}
		}

		// The only way to specify options on the idIndex is at collection creation time.
		// This loop pulls out the idIndex from `indexes` and sets it in `options`.
		for i, index := range indexes {
			// The index with the name "_id_" will always be the idIndex.
			if index.Options["name"].(string) == "_id_" {
				// Remove the index version (to use the default) unless otherwise specified.
				// If preserving UUID, we have to create a collection via
				// applyops, which requires the "v" key.
				if !restore.OutputOptions.KeepIndexVersion && !restore.OutputOptions.PreserveUUID {
					delete(index.Options, "v")
				}
				index.Options["ns"] = intent.Namespace()

				// If the collection has an idIndex, then we are about to create it, so
				// ignore the value of autoIndexId.
				for j, opt := range options {
					if opt.Name == "autoIndexId" {
						options = append(options[:j], options[j+1:]...)
					}
				}
				options = append(options, bson.DocElem{"idIndex", index})
				indexes = append(indexes[:i], indexes[i+1:]...)
				break
			}
		}

		if restore.OutputOptions.NoOptionsRestore {
			log.Logv(log.Info, "not restoring collection options")
			logMessageSuffix = "with no collection options"
			options = nil
		}
	}

	// TODO: Define logic to handle existing CosmosDB collection
	if !collectionExists {
		log.Logvf(log.Info, "creating collection %v %s", intent.Namespace(), logMessageSuffix)
		log.Logvf(log.DebugHigh, "using collection options: %#v", options)
		if strings.Contains(restore.ToolOptions.Host, ".documents.azure.com") {
			log.Logvf(log.Info, "We're targetting a Cosmos DB URI, let's create a custom collection")
			session, _ := restore.SessionProvider.GetSession()
			collection := session.DB(intent.DB).C(intent.C)
			err := cosmosdb.CreateCustomCosmosDB(cosmosdb.CosmosDBCollectionInfo{
				Throughput: restore.ToolOptions.General.Throughput,
				ShardKey:   restore.ToolOptions.General.ShardKey,
			}, collection)
			collection.Database.Session.Close()

			if err != nil {
				log.Logvf(log.Always, "Unable to create collection: %s", collection.Name)
				log.Logv(log.Always, "If the collection already exist please re-run the tool with `--drop` to delete the pre-existing collection")
				return err
			}
		} else {
			// TODO: Mongorestore seems to be creating collections with extra parameters, need to check if we need this as well
			if err = restore.CreateCollection(intent, options, uuid); err != nil {
				return fmt.Errorf("error creating collection %v: %v", intent.Namespace(), err)
			}
		}
	} else {
		log.Logvf(log.Info, "collection %v already exists - skipping collection create", intent.Namespace())
	}

	var documentCount int64
	if intent.BSONFile != nil {
		err = intent.BSONFile.Open()
		if err != nil {
			return err
		}
		defer intent.BSONFile.Close()

		log.Logvf(log.Always, "restoring %v from %v", intent.Namespace(), intent.Location)

		bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))
		defer bsonSource.Close()

		documentCount, err = restore.RestoreCollectionToDB(intent.DB, intent.C, bsonSource, intent.BSONFile, intent.Size)
		if err != nil {
			return fmt.Errorf("error restoring from %v: %v", intent.Location, err)
		}
	}

	// finally, add indexes
	if len(indexes) > 0 && !restore.OutputOptions.NoIndexRestore {
		log.Logvf(log.Always, "restoring indexes for collection %v from metadata", intent.Namespace())
		err = restore.CreateIndexes(intent, indexes)
		if err != nil {
			return fmt.Errorf("error creating indexes for %v: %v", intent.Namespace(), err)
		}
	} else {
		log.Logv(log.Always, "no indexes to restore")
	}

	log.Logvf(log.Always, "finished restoring %v (%v %v)",
		intent.Namespace(), documentCount, util.Pluralize(int(documentCount), "document", "documents"))
	return nil
}

// RestoreCollectionToDB pipes the given BSON data into the database.
// Returns the number of documents restored and any errors that occured.
func (restore *MongoRestore) RestoreCollectionToDB(dbName, colName string,
	bsonSource *db.DecodedBSONSource, file PosReader, fileSize int64) (int64, error) {

	var termErr error
	session, err := restore.SessionProvider.GetSession()
	if err != nil {
		return int64(0), fmt.Errorf("error establishing connection: %v", err)
	}
	session.SetSafe(restore.safety)
	defer session.Close()

	collection := session.DB(dbName).C(colName)

	documentCount := int64(0)
	watchProgressor := progress.NewCounter(fileSize)
	if restore.ProgressManager != nil {
		name := fmt.Sprintf("%v.%v", dbName, colName)
		restore.ProgressManager.Attach(name, watchProgressor)
		defer restore.ProgressManager.Detach(name)
	}

	maxInsertWorkers := restore.OutputOptions.NumInsertionWorkers
	if restore.OutputOptions.MaintainInsertionOrder {
		maxInsertWorkers = 1
	}

	docChan := make(chan bson.Raw, insertBufferFactor)
	backupDocChan := make(chan bson.Raw, insertBufferFactor)
	resultChan := make(chan error, maxInsertWorkers)

	// stream documents for this collection on docChan
	go func() {
		doc := bson.Raw{}
		for bsonSource.Next(&doc) {
			select {
			case <-restore.termChan:
				log.Logvf(log.Always, "terminating read on %v.%v", dbName, colName)
				termErr = util.ErrTerminated
				close(docChan)
				return
			default:
				rawBytes := make([]byte, len(doc.Data))
				copy(rawBytes, doc.Data)
				docChan <- bson.Raw{Data: rawBytes}
				documentCount++
			}
		}
		close(docChan)
	}()

	log.Logvf(log.Info, "using %v insertion workers", maxInsertWorkers)

	manager := cosmosdb.NewHiringManager(maxInsertWorkers, restore.ToolOptions.General.Throughput)
	manager.Action = cosmosdb.AddWorkerAction(func(hm *cosmosdb.HiringManager, workerId int) {

		s := session.Copy()
		defer s.Close()
		coll := collection.With(s)
		inserter := cosmosdb.NewCosmosDbInserter(coll)

		for {
			select {
			case rawDoc, alive := <-docChan:
				if !alive {
					log.Logvf(log.DebugLow, "Worker %d no more documents to ingest", workerId)
					return
				}

				if restore.objCheck {
					err := bson.Unmarshal(rawDoc.Data, &bson.D{})
					if err != nil {
						resultChan <- fmt.Errorf("invalid object: %v", err)
						return
					}
				}

				err := filterIngestError(restore.OutputOptions.StopOnError, inserter.Insert(rawDoc, hm, workerId))
				if err != nil {
					backupDocChan <- rawDoc
					return
				}

			case <-restore.termChan:
				log.Logvf(log.Always, "Worker %d recieved termination signal, stopping now", workerId)
				return
			}
			watchProgressor.Set(file.Pos())
		}
	})

	manager.Start(maxInsertWorkers, restore.ToolOptions.General.AutoScaleWorkers)
	manager.AwaitAllWorkers()
	close(backupDocChan)
	if len(backupDocChan) != 0 {
		log.Logvf(log.Always, "Trying to insert %d backup documents", len(backupDocChan))
		s := session.Copy()
		defer s.Close()
		coll := collection.With(s)
		inserter := cosmosdb.NewCosmosDbInserter(coll)
		for doc := range backupDocChan {

			if restore.objCheck {
				err := bson.Unmarshal(doc.Data, &bson.D{})
				if err != nil {
					resultChan <- fmt.Errorf("backup: invalid object: %v", err)
					break
				}
			}

			err := filterIngestError(restore.OutputOptions.StopOnError, inserter.Insert(doc, manager, 1))
			if err != nil {
				continue
			}
		}
	}
	s := session.Copy()
	defer s.Close()
	coll := collection.With(s)
	docCount, countErr := coll.Count()
	if countErr != nil {
		return 0, err
	}
	log.Logvf(log.Always, "Collection: %s has a total of %d documents in Azure Cosmos DB", collection.Name, docCount) // final error check
	if err = bsonSource.Err(); err != nil {
		return int64(0), fmt.Errorf("reading bson input: %v", err)
	}
	return documentCount, termErr
}

func filterIngestError(stopOnError bool, err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == io.EOF.Error() {
		return fmt.Errorf(db.ErrLostConnection)
	}
	if strings.Contains(err.Error(), "closed") {
		log.Logvf(log.Always, "Got a connection closed, trying to recover by letting another socket do the work")
		return err
	}
	if stopOnError || db.IsConnectionError(err) {
		return err
	}
	return nil
}
