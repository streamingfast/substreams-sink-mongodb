package sinker

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_postgres_progress_message", []string{"module"}, "The number of progress message received")
var BlockCount = metrics.NewCounter("substreams_sink_postgres_block_count", "The number of blocks received")
var FlushedEntriesCount = metrics.NewCounter("substreams_sink_postgres_flushed_entries_count", "The number of flushed entries")