package snapshot

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Synchronizer interface {
	Insert(ctx context.Context, item *primitive.D) error
	Update(ctx context.Context, source *primitive.D, target *primitive.D) error
	Delete(ctx context.Context, item *primitive.D) error
}

type MongoSynchronizer struct {
	Target     *mdb.MDB
	Database   string
	Collection string
}

func NewMongoSynchronizer(target *mdb.MDB, database string, collection string) *MongoSynchronizer {
	return &MongoSynchronizer{
		Target:     target,
		Database:   database,
		Collection: collection,
	}
}

func (s *MongoSynchronizer) Insert(ctx context.Context, item *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).InsertOne(ctx, item)
	return err
}

func (s *MongoSynchronizer) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).ReplaceOne(ctx, source, target)
	return err
}

func (s *MongoSynchronizer) Delete(ctx context.Context, item *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).DeleteOne(ctx, item)
	return err
}
