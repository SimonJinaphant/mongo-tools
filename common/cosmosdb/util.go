package cosmosdb

import (
	"fmt"
	"io"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
)

func BenchmarkTime(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Logvf(log.Always, "%s took %s", name, elapsed)
}

func FilterUnrecoverableErrors(stopOnError bool, err error) error {
	if err.Error() == io.EOF.Error() {
		return fmt.Errorf(db.ErrLostConnection)
	}
	if stopOnError || db.IsConnectionError(err) {
		return err
	}
	return nil
}
