package deltas

import (
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var zlog *zap.Logger

func init() {
	zlog, _ = logging.PackageLogger("database_v1", "github.com/streamingfast/substreams-databases/pb/substreams/databases/deltas/v1/")
}
