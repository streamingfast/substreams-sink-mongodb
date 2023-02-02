package sinker

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()
