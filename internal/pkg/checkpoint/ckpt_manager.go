package checkpoint

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CheckpointManager interface {
	GetCheckpoint(context.Context) (Checkpoint, error)
	StoreCheckpoint(context.Context, Checkpoint) error
}

type MongoCheckpoint struct {

	// Database used to store the checkpoint
	DB string

	// Collection used to store the checkpoint
	Collection string

	// In-memory storage of the current checkpoint
	Current Checkpoint
}

func NewMongoCheckpointService(db string, coll string) *MongoCheckpoint {
	return &MongoCheckpoint{
		DB:         db,
		Collection: coll,
	}
}

func (s *MongoCheckpoint) GetCheckpoint(ctx context.Context) (Checkpoint, error) {

	// Create a new client and connect to the server
	//connectOpts := options.Client().ApplyURI(config.Current.Repl.Target)
	// client, err := mongo.Connect(ctx, connectOpts)
	// if err != nil {
	// 	log.Fatal("Error connecting to the server: ", err)
	// }

	db := mong.Registry.GetTarget().Client.Database(s.DB)
	collection := db.Collection(s.Collection)

	filter := bson.M{}
	opts := options.FindOneOptions{
		Sort: map[string]int{"$natural": -1},
	}
	result := collection.FindOne(ctx, filter, &opts)
	err := result.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return Checkpoint{}, nil
		}
		log.Fatal("Error fetching the last LSN synched: ", err)
	}

	// Decode the result
	// var lsn *Lsn
	var ckpt Checkpoint = Checkpoint{}
	err = result.Decode(&ckpt)
	if err != nil {
		log.Fatal("Error decoding the result: ", err)
	}

	return ckpt, nil
}

func (s *MongoCheckpoint) StoreCheckpoint(ctx context.Context, c Checkpoint) error {

	// Store the checkpoint in memory
	s.Current = c

	// Store the checkpoint in the database
	opts := options.Update().SetUpsert(true)
	filter := bson.M{"name": c.Name}
	update := bson.M{"$set": s.Current}

	_, err := mong.Registry.GetTarget().Client.Database(s.DB).Collection(s.Collection).UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.WarnWithFields("Checkpoint upsert error", log.Fields{
			"checkpoint": c.Name,
			"updates":    update,
			"error":      err,
		})
		return err
	}
	return nil
}
