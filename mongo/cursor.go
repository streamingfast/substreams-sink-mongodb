package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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

	res := l.database.Collection("_cursors").FindOne(
		ctx,
		bson.M{"id": outputModuleHash},
	)

	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, ErrCursorNotFound
		}
		return nil, fmt.Errorf("getting cursor %q:  %w", outputModuleHash, res.Err())
	}

	var c cursorDocument
	if err := res.Decode(&c); err != nil {
		return nil, fmt.Errorf("decoding cursor %q:  %w", outputModuleHash, err)
	}

	return sink.NewBlankCursor(), nil
}

func (l *Loader) WriteCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	filter := bson.M{"id": moduleHash}
	update := bson.M{"$set": cursorDocument{Id: moduleHash, Cursor: c.Cursor, BlockNum: c.Block.Num(), BlockID: c.Block.ID()}}

	res, err := l.database.Collection("_cursors").UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("updating cursor %q:  %w", moduleHash, err)
	}

	if res.UpsertedCount > 0 || res.ModifiedCount > 0 {
		return nil
	}

	// else we need to insert it

	_, err = l.database.Collection("_cursors").InsertOne(ctx, cursorDocument{Id: moduleHash, Cursor: c.Cursor, BlockNum: c.Block.Num(), BlockID: c.Block.ID()})
	if err != nil {
		return fmt.Errorf("inserting cursor %q:  %w", moduleHash, err)
	}

	return nil
}
