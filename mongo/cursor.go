package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var ErrCursorNotFound = errors.New("cursor not found")

type cursorDocument struct {
	Id       string `bson:"id"`
	Cursor   string `bson:"cursor"`
	BlockNum uint64 `bson:"block_num"`
	BlockID  string `bson:"block_id"`
}

func (l *Loader) GetCursor(ctx context.Context, outputModuleHash string) (*sink.Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	l.logger.Debug("retrieving cursor from database",
		zap.String("module_hash", outputModuleHash))

	res := l.database.Collection("_cursors").FindOne(
		ctx,
		bson.M{"id": outputModuleHash},
	)

	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			l.logger.Debug("cursor not found in database",
				zap.String("module_hash", outputModuleHash))
			return nil, ErrCursorNotFound
		}
		return nil, fmt.Errorf("getting cursor %q:  %w", outputModuleHash, res.Err())
	}

	var c cursorDocument
	if err := res.Decode(&c); err != nil {
		return nil, fmt.Errorf("decoding cursor %q:  %w", outputModuleHash, err)
	}

	l.logger.Debug("cursor document found in database",
		zap.String("module_hash", c.Id),
		zap.Int("cursor_length", len(c.Cursor)),
		zap.Uint64("block_num", c.BlockNum),
		zap.String("block_id", c.BlockID))

	cursor, err := sink.NewCursor(c.Cursor)
	if err != nil {
		return nil, fmt.Errorf("creating cursor from string: %w", err)
	}

	l.logger.Debug("cursor successfully parsed",
		zap.Bool("is_blank", cursor.IsBlank()),
		zap.Stringer("block", cursor.Block()))

	return cursor, nil
}

func (l *Loader) WriteCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	l.logger.Debug("writing cursor to database",
		zap.String("module_hash", moduleHash),
		zap.Int("cursor_length", len(c.String())),
		zap.Uint64("block_num", c.Block().Num()),
		zap.String("block_id", c.Block().ID()),
		zap.Bool("is_blank", c.IsBlank()))

	doc := cursorDocument{
		Id:       moduleHash,
		Cursor:   c.String(),
		BlockNum: c.Block().Num(),
		BlockID:  c.Block().ID(),
	}

	filter := bson.M{"id": moduleHash}
	update := bson.M{"$set": doc}

	res, err := l.database.Collection("_cursors").UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upserting cursor %q: %w", moduleHash, err)
	}

	// Verify the operation succeeded
	if res.UpsertedCount > 0 || res.ModifiedCount > 0 || res.MatchedCount > 0 {
		l.logger.Debug("cursor successfully written",
			zap.String("module_hash", moduleHash),
			zap.Uint64("block_num", c.Block().Num()),
			zap.Bool("was_insert", res.UpsertedCount > 0),
			zap.Bool("was_update", res.ModifiedCount > 0))
		return nil
	}

	// Fallback: try explicit insert if update reports no operation
	_, err = l.database.Collection("_cursors").InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("inserting cursor %q: %w", moduleHash, err)
	}

	l.logger.Debug("cursor successfully inserted (fallback)",
		zap.String("module_hash", moduleHash),
		zap.Uint64("block_num", c.Block().Num()))

	return nil
}
