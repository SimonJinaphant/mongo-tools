// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/mongodb/mongo-tools/common/log"
)

// BufferedBulkInserter implements a bufio.Writer-like design for queuing up
// documents and inserting them in bulk when the given doc limit (or max
// message size) is reached. Must be flushed at the end to ensure that all
// documents are written.
type BufferedBulkInserter struct {
	bulk            *mgo.Bulk
	collection      *mgo.Collection
	continueOnError bool
	docLimit        int
	byteCount       int
	docCount        int
	unordered       bool
}

// NewBufferedBulkInserter returns an initialized BufferedBulkInserter
// for writing.
func NewBufferedBulkInserter(collection *mgo.Collection, docLimit int,
	continueOnError bool) *BufferedBulkInserter {
	bb := &BufferedBulkInserter{
		collection:      collection,
		continueOnError: continueOnError,
		docLimit:        docLimit,
	}
	bb.resetBulk()
	return bb
}

func (bb *BufferedBulkInserter) Unordered() {
	bb.unordered = true
	bb.bulk.Unordered()
}

// throw away the old bulk and init a new one
func (bb *BufferedBulkInserter) resetBulk() {
	bb.bulk = bb.collection.Bulk()
	if bb.continueOnError || bb.unordered {
		bb.bulk.Unordered()
	}
	bb.byteCount = 0
	bb.docCount = 0
}

// Insert adds a document to the buffer for bulk insertion. If the buffer is
// full, the bulk insert is made, returning any error that occurs.
func (bb *BufferedBulkInserter) Insert(doc interface{}, session *mgo.Session) error {
	rawBytes, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("bson encoding error: %v", err)
	}
	// flush if we are full
	if bb.docCount >= bb.docLimit || bb.byteCount+len(rawBytes) > MaxBSONSize {
		err = bb.FlushWithRetry(session)
	}
	// buffer the document
	bb.docCount++
	bb.byteCount += len(rawBytes)
	bb.bulk.Insert(bson.Raw{Data: rawBytes})
	return err
}

// FlushWithRetry continously writes all buffered documents in one bulk insert until there's no error then resets the buffer.
func (bb *BufferedBulkInserter) FlushWithRetry(session *mgo.Session) error {
	failedInsertCount := 0
	resetCount := 0
	if bb.docCount == 0 {
		return nil
	}
	defer bb.resetBulk()
retry:
	if _, err := bb.bulk.Run(); err != nil {
		if strings.Contains(err.Error(), "connection reset by peer") {
			resetCount++
			log.Logvf(log.Always, "Connection appears to be reset")
			session.New()
			if resetCount > 5 {
				log.Logv(log.Always, "Maximum socket retry exceeded; moving on")
				return err
			}
			goto retry

		}
		errMessage := ""
		if strings.Contains(err.Error(), "Request rate is large") ||
			strings.Contains(err.Error(), "The request rate is too large") {
			errMessage = "We're overloading Cosmos DB"
		} else if strings.Contains(err.Error(), "duplicate key error") {
			errMessage = "There's a duplicate?"
		} else if strings.Contains(err.Error(), "Partition key provided either doesn't correspond") {
			errMessage = "PartitionKey does not seem to correspond"
		} else if strings.Contains(err.Error(), "PartitionKey value must be supplied") {
			errMessage = "ParitionKey must be supplied"
		}
		if errMessage != "" {
			failedInsertCount++
			coolDownTime := 250 * failedInsertCount
			log.Logvf(log.Always, "%s; let's wait %d milliseconds", errMessage, coolDownTime)

			cooldownTimer := time.NewTimer(time.Duration(coolDownTime) * time.Millisecond)
			<-cooldownTimer.C
			if failedInsertCount > 30 {
				log.Logv(log.Always, "Maximum retry exceeded; moving on")
			} else {
				goto retry
			}
		}
		return err
	}
	return nil
}

// Flush writes all buffered documents in one bulk insert then resets the buffer.
func (bb *BufferedBulkInserter) Flush() error {
	if bb.docCount == 0 {
		return nil
	}
	defer bb.resetBulk()
	if _, err := bb.bulk.Run(); err != nil {
		return err
	}
	return nil
}
