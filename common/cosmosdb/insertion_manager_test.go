package cosmosdb

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/testutil"
	. "github.com/smartystreets/goconvey/convey"
)

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
