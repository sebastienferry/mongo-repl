package repl

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Lsn struct {
	// The timestamp of the operation
	LastLsnSynched int64 `json:"lastLsnSynched"`
}

// Get the last LSN synched from the target database
func GetLastLsnSynched(ctx context.Context) int64 {

	// Create a new client and connect to the server
	connectOpts := options.Client().ApplyURI(config.Current.Repl.Target)
	client, err := mongo.Connect(ctx, connectOpts)
	if err != nil {
		log.Fatal("Error connecting to the server: ", err)
	}

	db := client.Database(config.Current.Repl.Incr.State.Database)
	collection := db.Collection(config.Current.Repl.Incr.State.Collection)

	filter := bson.M{}
	opts := options.FindOneOptions{
		Sort: map[string]int{"$natural": -1},
	}
	result := collection.FindOne(ctx, filter, &opts)
	err = result.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0
		}
		log.Fatal("Error fetching the last LSN synched: ", err)
	}

	// Decode the result
	var lsn *Lsn
	err = result.Decode(lsn)
	if err != nil {
		log.Fatal("Error decoding the result: ", err)
	}

	if lsn != nil {
		return lsn.LastLsnSynched
	}
	return FullRepl
}
