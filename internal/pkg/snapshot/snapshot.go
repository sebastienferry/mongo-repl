package snapshot

import (
	"context"
	"sync"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Snapshot struct {
	ckpt checkpoint.CheckpointManager
}

func NewSnapshot(ckpt checkpoint.CheckpointManager) *Snapshot {
	return &Snapshot{
		ckpt: ckpt,
	}
}

func (s *Snapshot) RunSnapshots(ctx context.Context, dbAndCollections map[string][]string) {

	// Get the oplog windows
	oplogWindow, err := checkpoint.GetReplicasetOplogWindow()
	if err != nil {
		log.Fatal("error computing oplog window: ", err)
	}

	// Replicate the collections
	for db, cols := range dbAndCollections {

		var wg sync.WaitGroup
		var replErr error
		for _, collection := range cols {

			// Filter the collections to replicate
			// FilterIn has priority over FilterOut
			if len(config.Current.Repl.FiltersIn) > 0 {
				if _, ok := config.Current.Repl.FiltersIn[collection]; !ok {
					log.Info("skipping collection (filtered by configuration): ", collection)
					continue
				}
			} else if len(config.Current.Repl.FiltersOut) > 0 {
				if _, ok := config.Current.Repl.FiltersOut[collection]; ok {
					log.Info("skipping collection (filtered by configuration): ", collection)
					continue
				}
			}

			// Replicate the collection in a separate goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()
				if config.IsFeatureEnabled(config.DeltaReplication) {
					// Use the new delta replication
					or := mdb.NewMongoItemReader(mdb.Registry.GetSource(), db, collection)
					tr := mdb.NewMongoItemReader(mdb.Registry.GetTarget(), db, collection)
					tw := mdb.NewMongoWriter(mdb.Registry.GetTarget(), db, collection)
					delta := NewDeltaReplication(or, tr, tw, db, collection, true, config.Current.Repl.Full.BatchSize)
					delta.SynchronizeCollection(context.Background())
				} else {
					replErr = s.RunSnapshot(context.Background(), db, collection)
				}
			}()
		}

		// Wait for all the collections to finish
		wg.Wait()
		log.InfoWithFields("finished full replication", log.Fields{
			"database": db,
		})

		if replErr != nil {
			log.Fatal("error replicating the collections: ", replErr)
		}

		log.Info("oplog boundaries: ", oplogWindow)
		log.InfoWithFields("oplog dates:", log.Fields{
			"oldest": time.Unix(int64(oplogWindow.Oldest.T), 0),
			"newest": time.Unix(int64(oplogWindow.Newest.T), 0),
		})

		// As the full replication is finished, we can save the checkpoint
		s.ckpt.SetCheckpoint(ctx, oplogWindow.Newest, true)
	}
}

func (s *Snapshot) RunSnapshot(ctx context.Context, database string, collection string) error {

	writer := NewDocumentWriter(database, collection, mdb.Registry.GetTarget())
	reader := NewDocumentReader(database, collection, mdb.Registry.GetSource(),
		config.Current.Repl.Full.BatchSize, writer)

	// Keep track of the progress for reporting
	progress := NewSyncProgress(database, collection)
	writer.SetProgress(progress)
	reader.SetProgress(progress)

	// Start the replication
	err := reader.Replicate(ctx)
	if err != nil {
		log.Error("error replicating the collection: ", err)
		return err
	}

	// Replicate the indexes
	err = s.ReplicateIndexes(ctx, database, collection)
	if err != nil {
		log.Error("error replicating the indexes: ", err)
		return err
	}

	return err
}

// Replicates the indexes from the source to the target
func (s *Snapshot) ReplicateIndexes(ctx context.Context, database string, collection string) error {

	// Get the indexes from the source
	indexes, err := mdb.GetIndexesByDb(ctx, database, collection)
	if err != nil {
		log.Error("error getting the indexes: ", err)
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

		coll := mdb.Registry.GetTarget().Client.Database(database).Collection(collection)
		newName, err := coll.Indexes().CreateOne(ctx, newIndex)
		if err != nil {
			log.Error("error creating the index: ", err)
			return err
		} else {
			log.InfoWithFields("created index", log.Fields{"name": newName})
		}
	}
	return nil
}
