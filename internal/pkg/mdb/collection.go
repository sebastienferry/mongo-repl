package mdb

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
)

// get total count
var res struct {
	Count       int64   `bson:"count"`
	Size        float64 `bson:"size"`
	StorageSize float64 `bson:"storageSize"`
}

func GetCollectionStats(r *Mong, database, collection string) (uint64, error) {
	if err := r.Client.Database(database).RunCommand(nil,
		bson.D{{"collStats", collection}}).Decode(&res); err != nil {
		log.Error("Error getting collection stats: ", err)
		return 0, err
	}

	return uint64(res.Count), nil
}

// List the collections of a database
func ListCollections(ctx context.Context, db string, mongo *Mong) ([]string, error) {

	// List the collections
	collections, err := mongo.Client.Database(db).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	return collections, nil
}

func ListDbCollections(ctx context.Context, databases []string) (map[string][]string, error) {
	collections := make(map[string][]string)
	for _, db := range databases {
		c, err := ListCollections(ctx, db, Registry.GetSource())
		if err != nil {
			log.Fatal("Error getting the list of collections to replicate: ", err)
			return nil, err
		}
		collections[db] = c
	}
	return collections, nil
}
