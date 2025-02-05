package interfaces

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Defines the interface to read items from a source.
type ItemReader interface {

	// Read a batch of items from the source.
	ReadItems(ctx context.Context, batchSize int, boundaries ...primitive.ObjectID) ([]*bson.D, error)

	// Get the total number of items in the source.
	Count(ctx context.Context) (int64, error)
}
