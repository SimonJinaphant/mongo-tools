package cosmosdb

import (
	"time"

	"github.com/mongodb/mongo-tools/common/log"
)

func BenchmarkTime(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Logvf(log.Always, "%s took %s", name, elapsed)
}
