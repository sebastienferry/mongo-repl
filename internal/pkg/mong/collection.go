package mong

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// get total count
var res struct {
	Count       int64   `bson:"count"`
	Size        float64 `bson:"size"`
	StorageSize float64 `bson:"storageSize"`
}

// Get statistics of a collection
func GetStatsByCollection(r *Mong, database, collection string) (uint64, error) {
	if err := r.Client.Database(database).RunCommand(nil,
		bson.D{{"collStats", collection}}).Decode(&res); err != nil {
		log.Error("Error getting collection stats: ", err)
		return 0, err
	}

	return uint64(res.Count), nil
}

// List the collections of a database
func GetCollectionsByDb(ctx context.Context, db string, mongo *Mong) ([]string, error) {

	// List the collections
	collections, err := mongo.Client.Database(db).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	return collections, nil
}

// Get the list of collections to replicate
func GetCollections(ctx context.Context, databases []string) (map[string][]string, error) {
	collections := make(map[string][]string)
	for _, db := range databases {
		c, err := GetCollectionsByDb(ctx, db, Registry.GetSource())
		if err != nil {
			log.Fatal("Error getting the list of collections to replicate: ", err)
			return nil, err
		}
		collections[db] = c
	}
	return collections, nil
}

// GetIndexesByDb returns the indexes of a collection
func GetIndexesByDb(ctx context.Context, database string, collection string) ([]primitive.M, error) {
	var indexes []primitive.M
	cursor, err := Registry.GetSource().Client.Database(database).Collection(collection).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

// func CreateIndex(ctx context.Context, database string, collection string, index bson.M) error {
// 	_, err := Registry.GetTarget().Client.Database(database).Collection(collection).Indexes().CreateOne(ctx, index)
// 	if err != nil {
// 		log.Error("Error creating the index: ", err)
// 		return err
// 	}
// 	return nil
// }
