package sinker

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var FlushCount = metrics.NewCounter("substreams_sink_mongodb_store_flush_count", "The amount of flush that happened so far")
var FlushedEntriesCount = metrics.NewCounter("substreams_sink_mongodb_flushed_entries_count", "The number of flushed entries")
var FlushDuration = metrics.NewCounter("substreams_sink_mongodb_store_flush_duration", "The amount of time spent flushing cache to db (in nanoseconds)")
