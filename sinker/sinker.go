package sinker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-mongodb/mongo"
	pbdatabase "github.com/streamingfast/substreams-sink-mongodb/pb/substreams/sink/database/v1"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	BLOCK_PROGRESS      = 1000
	LIVE_BLOCK_PROGRESS = 1
)

type MongoSinker struct {
	*shutter.Shutter

	DBLoader *mongo.Loader
	Tables   mongo.Tables

	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig

	UndoBufferSize  int
	LivenessTracker *sink.LivenessChecker

	sink       *sink.Sinker
	lastCursor *sink.Cursor

	blockRange *bstream.Range

	logger *zap.Logger
	tracer logging.Tracer
}

type Config struct {
	DBLoader *mongo.Loader
	DBName   string

	DDL mongo.Tables

	UndoBufferSize     int
	LiveBlockTimeDelta time.Duration

	BlockRange       string
	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig
}

func New(config *Config, logger *zap.Logger, tracer logging.Tracer) (*MongoSinker, error) {
	s := &MongoSinker{
		Shutter: shutter.New(),
		logger:  logger,
		tracer:  tracer,

		Tables: config.DDL,

		DBLoader:         config.DBLoader,
		Pkg:              config.Pkg,
		OutputModule:     config.OutputModule,
		OutputModuleName: config.OutputModuleName,
		OutputModuleHash: config.OutputModuleHash,
		ClientConfig:     config.ClientConfig,

		UndoBufferSize:  config.UndoBufferSize,
		LivenessTracker: sink.NewLivenessChecker(config.LiveBlockTimeDelta),
	}

	s.OnTerminating(func(err error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Stop(ctx, err)
	})

	var err error
	s.blockRange, err = resolveBlockRange(config.BlockRange, config.OutputModule)
	if err != nil {
		return nil, fmt.Errorf("resolve block range: %w", err)
	}

	return s, nil
}

func (s *MongoSinker) Stop(ctx context.Context, err error) {
	if s.lastCursor == nil || err != nil {
		return
	}

	_ = s.DBLoader.WriteCursor(ctx, hex.EncodeToString(s.OutputModuleHash), s.lastCursor)
}

func (s *MongoSinker) Start(ctx context.Context) error {
	return s.Run(ctx)
}

func (s *MongoSinker) Run(ctx context.Context) error {
	cursor, err := s.DBLoader.GetCursor(ctx, hex.EncodeToString(s.OutputModuleHash))
	if err != nil && !errors.Is(err, mongo.ErrCursorNotFound) {
		return fmt.Errorf("unable to get cursor: %w", err)
	}

	if errors.Is(err, mongo.ErrCursorNotFound) {
		cursorStartBlock := s.OutputModule.InitialBlock
		if s.blockRange.StartBlock() > 0 {
			cursorStartBlock = s.blockRange.StartBlock() - 1
		}

		cursor = sink.NewCursor("", bstream.NewBlockRef("", cursorStartBlock))

		if err = s.DBLoader.WriteCursor(ctx, hex.EncodeToString(s.OutputModuleHash), cursor); err != nil {
			return fmt.Errorf("failed to create initial cursor: %w", err)
		}
	}

	var sinkOptions []sink.Option
	if s.UndoBufferSize > 0 {
		sinkOptions = append(sinkOptions, sink.WithBlockDataBuffer(s.UndoBufferSize))
	}

	s.sink, err = sink.New(
		sink.SubstreamsModeDevelopment, // fixme: change back to production mode
		s.Pkg.Modules,
		s.OutputModule,
		s.OutputModuleHash,
		s.handleBlockScopeData,
		s.ClientConfig,
		[]pbsubstreams.ForkStep{
			pbsubstreams.ForkStep_STEP_NEW,
			pbsubstreams.ForkStep_STEP_UNDO,
			pbsubstreams.ForkStep_STEP_IRREVERSIBLE,
		},
		s.logger,
		s.tracer,
		sinkOptions...,
	)
	if err != nil {
		return fmt.Errorf("unable to create sink: %w", err)
	}

	s.sink.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.logger.Info("terminating sink")
		s.sink.Shutdown(err)
	})

	if err := s.sink.Start(ctx, s.blockRange, cursor); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	return nil
}

func (s *MongoSinker) applyDatabaseChanges(ctx context.Context, blockId string, blockNum uint64, databaseChanges *pbdatabase.DatabaseChanges) (err error) {
	for _, change := range databaseChanges.TableChanges {
		id := change.Pk
		switch change.Operation {
		case pbdatabase.TableChange_UNSET:
		case pbdatabase.TableChange_CREATE:
			entity := map[string]interface{}{}
			for _, field := range change.Fields {
				var newValue interface{} = field.NewValue
				if fs, found := s.Tables[change.Table]; found {
					if f, found := fs[field.Name]; found {
						switch f {
						case mongo.INTEGER:
							newValue, err = strconv.ParseInt(field.NewValue, 10, 64)
							if err != nil {
								return
							}
						case mongo.DOUBLE:
							newValue, err = strconv.ParseFloat(field.NewValue, 64)
							if err != nil {
								return
							}
						case mongo.BOOLEAN:
							newValue, err = strconv.ParseBool(field.NewValue)
							if err != nil {
								return
							}
						case mongo.TIMESTAMP:
							var tempValue int64
							tempValue, err = strconv.ParseInt(field.NewValue, 10, 64)
							if err != nil {
								return
							}
							newValue = time.Unix(tempValue, 0)
						case mongo.NULL:
							if field.NewValue != "" {
								return
							}
							newValue = nil
						case mongo.DATE:
							var tempValue time.Time
							tempValue, err = time.Parse(time.RFC3339, field.NewValue)
							newValue = tempValue
						default:
							// string
						}
					}
				}
				entity[field.Name] = newValue
			}
			err := s.DBLoader.Save(ctx, change.Table, id, entity)
			if err != nil {
				return fmt.Errorf("saving entity %s with id %s: %w (block num: %d id %s)", change.Table, id, err, blockNum, blockId)
			}
		case pbdatabase.TableChange_UPDATE:
			entityChanges := map[string]interface{}{}
			for _, field := range change.Fields {
				entityChanges[field.Name] = field.NewValue
			}
			err := s.DBLoader.Update(ctx, change.Table, change.Pk, entityChanges)
			if err != nil {
				return fmt.Errorf("updating entity %s with id %s: %w (block num: %d id %s)", change.Table, id, err, blockNum, blockId)
			}
		case pbdatabase.TableChange_DELETE:
			for range change.Fields {
				err := s.DBLoader.Delete(ctx, change.Table, change.Pk)
				if err != nil {
					return fmt.Errorf("deleting entity %s with id %s: %w (block num: %d id %s)", change.Table, id, err, blockNum, blockId)
				}
			}
		}
	}

	return nil
}

func (s *MongoSinker) handleBlockScopeData(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {
	for _, output := range data.Outputs {
		if output.Name != s.OutputModuleName {
			continue
		}

		dbChanges := &pbdatabase.DatabaseChanges{}
		err := proto.Unmarshal(output.GetMapOutput().GetValue(), dbChanges)
		if err != nil {
			return fmt.Errorf("unmarshal database changes: %w", err)
		}

		err = s.applyDatabaseChanges(ctx, data.Clock.Id, data.Clock.Number, dbChanges)
		if err != nil {
			return fmt.Errorf("apply database changes: %w", err)
		}
	}

	s.lastCursor = cursor

	if cursor.Block.Num()%s.batchBlockModulo(data) == 0 {
		if err := s.DBLoader.WriteCursor(ctx, hex.EncodeToString(s.OutputModuleHash), cursor); err != nil {
			return fmt.Errorf("failed to roll: %w", err)
		}
	}

	return nil
}

func (s *MongoSinker) batchBlockModulo(blockData *pbsubstreams.BlockScopedData) uint64 {
	if s.LivenessTracker.IsLive(blockData) {
		return LIVE_BLOCK_PROGRESS
	}
	return BLOCK_PROGRESS
}
