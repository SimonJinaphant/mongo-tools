package cosmosdb

import (
	"fmt"
	"testing"

	"github.com/mongodb/mongo-tools/common/testutil"
	. "github.com/smartystreets/goconvey/convey"
)

func TestWorkerMessaging(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	worker := NewInsertionWorker(nil, 0, false)

	Convey("When the manager sends a message", t, func() {
		Convey("to slow down, the worker should increase its wait time", func() {
			worker.handleMessage(MsgSlowdown)
			So(worker.waitTime, ShouldEqual, awaitBetweenOpTime)
		})

		Convey("to speed back up, the worker should decrease its wait time", func() {
			worker.handleMessage(MsgSpeedup)
			So(worker.waitTime, ShouldEqual, 0)
		})

		So(worker.shouldSendStatistics, ShouldEqual, false)
		Convey("to send statistics, the worker should prepare to send them", func() {
			worker.handleMessage(MsgRequestSample)
			So(worker.shouldSendStatistics, ShouldEqual, true)
		})
	})
}

func TestWorkerBackup(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	worker := NewInsertionWorker(nil, 0, false)
	backupChannel := make(chan interface{}, 1)
	worker.backupChannel = backupChannel
	var dummy interface{}

	Convey("When the manager inserts a backup document", t, func() {
		Convey("that wasn't previously inserted", func() {
			err := worker.handleBackup(dummy, nil)
			So(err, ShouldBeNil)
		})

		Convey("but it was previously inserted", func() {
			err := worker.handleBackup(dummy, fmt.Errorf("duplicate key"))
			So(err, ShouldBeNil)
		})

		Convey("but it failed to insert again due to a timeout", func() {
			So(len(backupChannel), ShouldEqual, 0)
			err := worker.handleBackup(dummy, fmt.Errorf("ExceedInsertDeadline"))
			So(err, ShouldBeNil)
			So(len(backupChannel), ShouldEqual, 1)
		})
	})
}
