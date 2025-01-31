package snapshot

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ItemReader interface {
	ReadItems(ctx context.Context, batchSize int, startId primitive.ObjectID) ([]*bson.D, error)
}

type MongoItemReader struct {
	Database   string
	Collection string
	Source     *mdb.MDB
}

func NewMongoItemReader(source *mdb.MDB, database string, collection string) *MongoItemReader {
	return &MongoItemReader{
		Source:     source,
		Database:   database,
		Collection: collection,
	}
}

// Reads a batch of items from the database starting with the next ID after the `first`
// and sorted ascendingly by ID
func (r *MongoItemReader) ReadItems(ctx context.Context, batchSize int,
	first primitive.ObjectID) ([]*bson.D, error) {

	// Initialize a result
	items := make([]*bson.D, 0, batchSize)

	// Prepare the find statement
	findOptions := new(options.FindOptions)
	findOptions.SetSort(map[string]interface{}{
		"_id": 1,
	})
	findOptions.SetLimit(int64(batchSize))

	// Filter the documents
	filter := bson.D{{
		Key: "_id", Value: bson.D{{
			Key: "$gt", Value: first,
		}},
	}}

	// Read the documents
	cur, err := r.Source.Client.Database(r.Database).Collection(r.Collection).Find(ctx, filter, findOptions)
	if err != nil {
		return items, err
	}

	// Prepare a buffer to store documents to sync
	for cur.Next(ctx) {

		if err := cur.Err(); err != nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return items, err
		}

		var item *bson.D = &bson.D{}
		err := cur.Decode(item)

		if err != nil || item == nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return items, err
		}

		items = append(items, item)
	}
	return items, nil
}
