package mongo

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Tables map[string]Fields
type Fields map[string]DatabaseType
type DatabaseType string

const (
	INTEGER   DatabaseType = "integer"
	DOUBLE    DatabaseType = "double"
	BOOLEAN   DatabaseType = "boolean"
	TIMESTAMP DatabaseType = "timestamp"
	NULL      DatabaseType = "null"
	DATE      DatabaseType = "date"
)

type Loader struct {
	client   *mongo.Client
	database *mongo.Database
	tables   Tables

	cursorCollectionName string
	entityCollectionName string

	logger *zap.Logger
}

func NewMongoDB(address string, databaseName string, logger *zap.Logger) (*Loader, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(address))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &Loader{client: client, database: client.Database(databaseName), logger: logger}, nil
}

func (l *Loader) Ping(ctx context.Context) error {
	return l.client.Ping(ctx, nil)
}

func (l *Loader) Save(ctx context.Context, collectionName string, id string, entity map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	collection := l.database.Collection(collectionName)
	update := bson.M{"$set": entity}

	res, err := collection.UpdateByID(ctx, id, update, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	if res.UpsertedCount == 0 {
		return errors.New("no document inserted")
	}

	return nil
}

func (l *Loader) Update(ctx context.Context, collectionName string, id string, changes map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	collection := l.database.Collection(collectionName)
	update := bson.M{"$set": changes}

	res, err := collection.UpdateByID(ctx, id, update, options.Update().SetUpsert(false))
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 && res.UpsertedCount == 0 && res.ModifiedCount == 0 && res.UpsertedID == nil {
		return errors.New("no document updated")
	}

	return nil
}

func (l *Loader) Delete(ctx context.Context, collectionName string, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	collection := l.database.Collection(collectionName)
	filter := bson.M{"id": id}
	res, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}

	if res.DeletedCount == 0 {
		return errors.New("no document deleted")
	}

	return nil
}
