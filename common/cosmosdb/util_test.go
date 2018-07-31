package cosmosdb

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/testutil"
	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateSizeRequirement(t *testing.T) {

	testutil.VerifyTestType(t, testutil.UnitTestType)

	Convey("Given a Shard Key that's not empty", t, func() {
		Convey("and a file size not over 2GB", func() {
			err := ValidateSizeRequirement("abc", 2*gigabyte, false)
			So(err, ShouldBeNil)
		})

		Convey("and a file size between 2GB and 4GB", func() {
			err := ValidateSizeRequirement("abc", 3*gigabyte, false)
			So(err, ShouldBeNil)
		})

		Convey("and a file size greater than 4GB", func() {
			err := ValidateSizeRequirement("abc", 4*gigabyte+1, false)
			So(err, ShouldBeNil)
		})
	})

	Convey("When no shard key is specified", t, func() {
		Convey("and the import file is not over 2GB", func() {
			err := ValidateSizeRequirement("", 2*gigabyte, false)
			So(err, ShouldBeNil)
		})

		Convey("and the import file is between 2GB and 4GB", func() {
			err := ValidateSizeRequirement("", 3*gigabyte, false)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, fileSizeWarningErrMsg)
		})

		Convey("and the import file is between 2GB and 4GB with no file size supression", func() {
			err := ValidateSizeRequirement("", 3*gigabyte, true)
			So(err, ShouldBeNil)
		})

		Convey("and the import file is size greater than 4GB", func() {
			err := ValidateSizeRequirement("", 4*gigabyte+1, false)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, fileSizeFailureErrMsg)
		})

		Convey("and the import file is size greater than 4GBwith no file size supression", func() {
			err := ValidateSizeRequirement("", 4*gigabyte+1, true)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, fileSizeFailureErrMsg)
		})
	})
}
