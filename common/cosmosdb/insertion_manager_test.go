package cosmosdb

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/testutil"
	. "github.com/smartystreets/goconvey/convey"
)

func setupChannelsForSlowdownAndSpeedup() *InsertionManager {
	manager := NewInsertionManager(nil, nil, false, false)

	manager.managerChannels = append(manager.managerChannels, nil)
	manager.managerChannels[0] = make(chan InsertionManagerMessage, 1)

	manager.managerChannels = append(manager.managerChannels, nil)
	manager.managerChannels[1] = make(chan InsertionManagerMessage, 1)

	manager.workerCount = 2
	return manager
}

func TestVerifyMassHiringChoice(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	Convey("When there is no throtting, the mass hiring manager should", t, func() {
		Convey("hire the exact amount of workers given if it does not exceed the max hiring cap", func() {
			hireCount, err := verifyMassHiringChoices(massHiringMaxWorkerPerHire, 0, 0)
			So(err, ShouldBeNil)
			So(hireCount, ShouldEqual, massHiringMaxWorkerPerHire)
		})

		Convey("not hire anymore than the max hiring cap", func() {
			hireCount, err := verifyMassHiringChoices(massHiringMaxWorkerPerHire+1, 0, 0)
			So(err, ShouldBeNil)
			So(hireCount, ShouldEqual, massHiringMaxWorkerPerHire)
		})
	})

	Convey("The mass hiring manager should", t, func() {
		Convey("not hire any worker if it was recently rate limited", func() {
			hireCount, err := verifyMassHiringChoices(massHiringMaxWorkerPerHire+1, 1, 0)
			So(err, ShouldNotBeNil)
			So(hireCount, ShouldEqual, 0)
		})

		Convey("not hire any worker if there is a worker already slowed down", func() {
			hireCount, err := verifyMassHiringChoices(massHiringMaxWorkerPerHire+1, 0, 1)
			So(err, ShouldNotBeNil)
			So(hireCount, ShouldEqual, 0)
		})
	})
}

func TestSignalSlowdown(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	manager := setupChannelsForSlowdownAndSpeedup()
	channelA := manager.managerChannels[0]
	channelB := manager.managerChannels[1]

	Convey("Insertion manager should signal a slowdown when it gets throttled", t, func() {

		Convey("to the 1st worker", func() {
			manager.signalSlowdown()

			So(len(channelA), ShouldEqual, 1)
			So(len(channelB), ShouldEqual, 0)
			resultA := <-channelA
			So(resultA, ShouldEqual, MsgSlowdown)
			So(len(channelA), ShouldEqual, 0)
		})

		Convey("and then to the 2nd worker if it gets throttled again", func() {
			manager.signalSlowdown()

			So(len(channelA), ShouldEqual, 0)
			So(len(channelB), ShouldEqual, 1)
			resultB := <-channelB
			So(resultB, ShouldEqual, MsgSlowdown)
			So(len(channelB), ShouldEqual, 0)
		})

		Convey("and round-robin back to the 1st worker if it gets throttled yet again", func() {
			manager.signalSlowdown()

			So(len(channelA), ShouldEqual, 1)
			So(len(channelB), ShouldEqual, 0)
			resultA := <-channelA
			So(resultA, ShouldEqual, MsgSlowdown)
			So(len(channelA), ShouldEqual, 0)
		})
	})
}

func TestSignalSpeedup(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	manager := setupChannelsForSlowdownAndSpeedup()
	manager.slowDownCount = 3
	channelA := manager.managerChannels[0]
	channelB := manager.managerChannels[1]

	Convey("Insertion manager should signal a speedup", t, func() {

		Convey("to the 1st worker", func() {
			manager.signalSpeedup()

			So(len(channelA), ShouldEqual, 1)
			So(len(channelB), ShouldEqual, 0)
			resultA := <-channelA
			So(resultA, ShouldEqual, MsgSpeedup)
			So(len(channelA), ShouldEqual, 0)
		})

		Convey("and then to the 2nd worker on the next consecutive speedup", func() {
			manager.signalSpeedup()

			So(len(channelA), ShouldEqual, 0)
			So(len(channelB), ShouldEqual, 1)
			resultB := <-channelB
			So(resultB, ShouldEqual, MsgSpeedup)
			So(len(channelB), ShouldEqual, 0)
		})

		Convey("and the 1st worker again on the next consecutive speedup", func() {
			manager.signalSpeedup()

			So(len(channelA), ShouldEqual, 1)
			So(len(channelB), ShouldEqual, 0)
			resultA := <-channelA
			So(resultA, ShouldEqual, MsgSpeedup)
			So(len(channelA), ShouldEqual, 0)
		})

		Convey("nothing when all workers are at full speed", func() {
			manager.signalSpeedup()

			So(len(channelA), ShouldEqual, 0)
			So(len(channelB), ShouldEqual, 0)
		})
	})
}
