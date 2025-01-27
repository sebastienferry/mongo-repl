package snapshot

import (
	"context"
	"sync"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func RunSnapshots(ctx context.Context, checkpointManager checkpoint.CheckpointManager,
	dbAndCollections map[string][]string) {

	// Get the oplog windows
	oplogWindow, err := checkpoint.GetReplicasetOplogWindow()
	if err != nil {
		log.Fatal("Error computing the oplog window: ", err)
	}

	// Replicate the collections
	for db, cols := range dbAndCollections {

		var wg sync.WaitGroup
		var replErr error
		for _, collection := range cols {

			// Filter the collections to replicate
			// FilterIn has priority over FilterOut
			if len(config.Current.Repl.Filters["in"]) > 0 {
				if _, ok := config.Current.Repl.FiltersIn[collection]; !ok {
					log.Info("Skipping collection: ", collection)
					continue
				}
			} else if len(config.Current.Repl.FiltersOut) > 0 {
				if _, ok := config.Current.Repl.FiltersOut[collection]; ok {
					log.Info("Skipping collection: ", collection)
					continue
				}
			}

			// Replicate the collection in a separate goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()
				replErr = RunSnapshot(context.Background(), db, collection)
			}()
		}

		// Wait for all the collections to finish
		wg.Wait()
		log.InfoWithFields("Finished full replication for database", log.Fields{
			"database": db,
		})

		if replErr != nil {
			log.Fatal("Error replicating the collections: ", replErr)
		}

		log.Info("OPLog boundaries: ", oplogWindow)
		log.InfoWithFields("OPLog dates:", log.Fields{
			"oldest": time.Unix(int64(oplogWindow.Oldest.T), 0),
			"newest": time.Unix(int64(oplogWindow.Newest.T), 0),
		})

		// As the full replication is finished, we can save the checkpoint
		checkpointManager.SetCheckpoint(ctx, oplogWindow.Newest, true)
	}
}

func RunSnapshot(ctx context.Context, database string, collection string) error {

	writer := NewDocumentWriter(database, collection, mong.Registry.GetTarget())
	reader := NewDocumentReader(database, collection, mong.Registry.GetSource(),
		config.Current.Repl.Full.BatchSize, writer)

	// Keep track of the progress for reporting
	progress := NewSyncProgress(database, collection)
	writer.SetProgress(progress)
	reader.SetProgress(progress)

	// Start the replication
	err := reader.Replicate(ctx)
	if err != nil {
		log.Error("Error replicating the collection: ", err)
		return err
	}

	// Replicate the indexes
	err = ReplicateIndexes(ctx, database, collection)
	if err != nil {
		log.Error("Error replicating the indexes: ", err)
		return err
	}

	return err
}

// Replicates the indexes from the source to the target
func ReplicateIndexes(ctx context.Context, database string, collection string) error {

	// Get the indexes from the source
	indexes, err := mong.GetIndexesByDb(ctx, database, collection)
	if err != nil {
		log.Error("Error getting the indexes: ", err)
		return err
	}

	// Create the indexes on the target
	for _, index := range indexes {

		if index["name"] == "_id_" {
			continue
		}

		// IndexModel Keys receives an ordered list of bson.D
		var keys bson.D = bson.D{}
		name := index["name"].(string)
		indexed := index["key"].(primitive.M)

		unique := false
		val, ok := index["unique"]
		if ok {
			unique = val.(bool)
		}

		for k, v := range indexed {
			keys = append(keys, bson.E{Key: k, Value: v})
		}

		opts := options.Index()
		opts.SetName(name)
		opts.SetUnique(unique)
		newIndex := mongo.IndexModel{
			Keys:    keys,
			Options: opts,
		}

		coll := mong.Registry.GetTarget().Client.Database(database).Collection(collection)
		newName, err := coll.Indexes().CreateOne(ctx, newIndex)
		if err != nil {
			log.Error("Error creating the index: ", err)
			return err
		} else {
			log.InfoWithFields("Created index", log.Fields{"name": newName})
		}
	}
	return nil
}
