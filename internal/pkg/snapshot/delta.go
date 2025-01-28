package snapshot

import (
	"bytes"
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DeltaReplication struct {
}

func Replicate(ctx context.Context, database string, collection string) error {
	// Take the X first elements from source and destination

	startId := primitive.ObjectID{}
	for {
		// Read the source items
		sourceItems, highestId, err := ReadItems(ctx, 5, mdb.Registry.GetSource(), startId, database, collection)
		if err != nil {
			log.Error("Error reading source items: ", err)
			return err
		}
		if len(sourceItems) == 0 {
			break
		}
		startId = highestId

		for _, item := range sourceItems {
			log.Info("Item: ", item)
		}

	}
	return nil
}

func ReadItems(ctx context.Context, batchSize int, db *mdb.MDB,
	first primitive.ObjectID, database string, collection string) ([]*bson.D, primitive.ObjectID, error) {

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
	cur, err := db.Client.Database(database).Collection(collection).Find(ctx, filter, findOptions)
	if err != nil {
		return nil, primitive.ObjectID{}, err
	}

	// Prepare a buffer to store documents to sync
	buffer := make([]*bson.D, 0, batchSize)
	high := first
	for cur.Next(ctx) {

		if err := cur.Err(); err != nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return nil, primitive.ObjectID{}, err
		}

		var item *bson.D = &bson.D{}
		err := cur.Decode(item)

		if err != nil || item == nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return nil, primitive.ObjectID{}, err
		}

		// Update the highest ID
		for _, elem := range *item {
			// In general, this is the first element of the document
			if elem.Key == "_id" {
				if oid, ok := elem.Value.(primitive.ObjectID); ok {

					// ObjectID is a byte array of 12 bytes
					// Compare the bytes to find the highest ID
					if bytes.Compare(oid[0:12], high[0:12]) > 0 {
						high = oid
					}
					// for i := 0; i < 12; i++ {
					// 	if oid[i] > high[i] {
					// 		high = oid
					// 		break
					// 	}
					// }

					// Timestamp extract the time part of the ObjectId.
					// if oid.Timestamp().After(high.Timestamp()) {
					// 	high = oid
					// }
				}
				break
			}
		}

		buffer = append(buffer, item)
	}

	return buffer, high, nil
}
