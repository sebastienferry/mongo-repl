package mdb

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Defines a structure to implement the ItemReader interface for MongoDB.
type MongoItemReader struct {
	Database   string // Database to read from
	Collection string // Collection to read from
	Source     *MDB   // Source MongoDB client
}

func NewMongoItemReader(source *MDB, database string, collection string) *MongoItemReader {
	return &MongoItemReader{
		Source:     source,
		Database:   database,
		Collection: collection,
	}
}

// Counts the number of items in the database
func (r *MongoItemReader) Count(ctx context.Context) (int64, error) {
	return GetDocumentCountByCollection(r.Source, r.Database, r.Collection)
}

// Reads a batch of items from the database starting with the next ID after the `first`
// and sorted ascendingly by ID
func (r *MongoItemReader) ReadItems(ctx context.Context, batchSize int,
	boundaries ...primitive.ObjectID) ([]*bson.D, error) {

	if len(boundaries) == 0 {
		return nil, nil
	}

	first, last := ComputeIdsWindow(boundaries...)

	// Initialize a result
	items := make([]*bson.D, 0, batchSize)

	// Prepare the find statement
	findOptions := new(options.FindOptions)
	findOptions.SetSort(map[string]interface{}{
		"_id": 1,
	})
	findOptions.SetLimit(int64(batchSize))

	// Filter the documents
	var filter bson.D
	if !last.IsZero() {
		filter = bson.D{{
			Key: "_id", Value: bson.D{{
				Key: "$gt", Value: first,
			}, {
				Key: "$lte", Value: last,
			}},
		}}
	} else {
		filter = bson.D{{
			Key: "_id", Value: bson.D{{
				Key: "$gt", Value: first,
			}},
		}}
	}

	// Read the documents
	cur, err := r.Source.GetClient(ctx).Database(r.Database).Collection(r.Collection).Find(ctx, filter, findOptions)
	if err != nil {
		return items, err
	}

	// Prepare a buffer to store documents to sync
	for cur.Next(ctx) {

		if err := cur.Err(); err != nil {
			log.Error("error reading document: ", err)
			cur.Close(ctx)
			return items, err
		}

		raw := cur.Current
		if raw == nil {
			log.Error("error reading document: ", err)
			cur.Close(ctx)
		}

		var item *bson.D = &bson.D{}
		err := cur.Decode(item)

		// Successfully read a batch of documents. Increment the counter
		metrics.SnapshotReadCounter.WithLabelValues(r.Database, r.Collection).Inc()

		if err != nil || item == nil {
			log.Error("error reading document: ", err)
			cur.Close(ctx)
			return items, err
		}

		items = append(items, item)
	}
	return items, nil
}
