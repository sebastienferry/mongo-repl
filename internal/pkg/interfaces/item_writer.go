package interfaces

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BulkResult struct {
	InsertedCount           int
	UpdatedCount            int
	DeletedCount            int
	SkippedOnDuplicateCount int
	ErrorCount              int
}

type ItemWriter interface {
	Insert(ctx context.Context, item *primitive.D) error
	InsertMany(ctx context.Context, items []*bson.D) (BulkResult, error)
	Update(ctx context.Context, source *primitive.D, target *primitive.D) error
	UpdateMany(ctx context.Context, items []*bson.D) (BulkResult, error)
	Delete(ctx context.Context, id primitive.ObjectID) error
	DeleteMany(ctx context.Context, ids []primitive.ObjectID) (BulkResult, error)
	WriteMany(ctx context.Context, items []*bson.D) (BulkResult, error)
}
