package checkpoint

import (
	"context"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CheckpointManager interface {
	GetCheckpoint(context.Context) (Checkpoint, error)
	SetCheckpoint(context.Context, primitive.Timestamp, bool) error
	MoveCheckpointForward(primitive.Timestamp)
	StartAutosave(context.Context)
	StopAutosave()
}

type MongoCheckpoint struct {

	// Database used to store the checkpoint
	DB string

	// Collection used to store the checkpoint
	Collection string

	// In-memory storage of the current checkpoint
	Current Checkpoint

	// Autosave stop
	autosave chan bool
}

func NewMongoCheckpointService(name string, ckptDb string, ckptColl string) *MongoCheckpoint {

	if name == "" {
		name = "default"
	}

	return &MongoCheckpoint{
		DB:         ckptDb,
		Collection: ckptColl,
		autosave:   make(chan bool),
		Current: Checkpoint{
			Name: name,
		},
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

	s.Current = ckpt
	return ckpt, nil
}

func (s *MongoCheckpoint) SetCheckpoint(ctx context.Context, ts primitive.Timestamp, save bool) error {

	// Store the checkpoint in memory
	s.MoveCheckpointForward(ts)

	// Save the checkpoint if requested
	if save {
		return s.saveCheckpoint(ctx)
	}

	return nil
}

func (s *MongoCheckpoint) MoveCheckpointForward(ts primitive.Timestamp) {

	if ts.T == 0 || ts.T < s.Current.LatestTs.T {
		log.Warn("Invalid timestamp: ", ts)
		return
	}

	// TODO: Should not be the following operation an atomic one?
	// We have the autosave running. We should stop it, update the checkpoint and restart it.
	s.Current.LatestTs = ts
	s.Current.Latest = ToDate(ts)
	s.Current.LatestLSN = ToInt64(ts)
	s.Current.SavedAt = time.Now()
}

func (s *MongoCheckpoint) saveCheckpoint(ctx context.Context) error {

	// Change the saved information
	s.Current.SavedAt = time.Now()

	// Store the checkpoint in the database
	opts := options.Update().SetUpsert(true)
	filter := bson.M{"name": s.Current.Name}
	update := bson.M{"$set": s.Current}

	_, err := mong.Registry.GetTarget().Client.Database(s.DB).Collection(s.Collection).UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.WarnWithFields("Checkpoint upsert error", log.Fields{
			"checkpoint": s.Current.Name,
			"updates":    update,
			"error":      err,
		})
		return err
	}
	return nil
}

func (s *MongoCheckpoint) StartAutosave(context.Context) {

	log.Info("Starting autosave")
	go func() {
		for {

			// Check if the autosave has been stopped
			select {
			case <-s.autosave:
				return
			default:
			}

			// Store the checkpoint
			s.saveCheckpoint(context.Background())
			log.Info("Checkpoint autosaved: ", s.Current)
			time.Sleep(10 * time.Second)
		}
	}()
}

func (s *MongoCheckpoint) StopAutosave() {
	s.autosave <- true
}
