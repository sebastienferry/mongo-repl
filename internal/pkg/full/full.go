package full

import (
	"context"
	"sync"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
)

func StartFullReplication(ctx context.Context, checkpointManager checkpoint.CheckpointManager,
	dbAndCollections map[string][]string) {

	// Keep track of the max timestamp for each collection
	// var ckptMap map[string]TimestampNode
	// var err error

	// Compute the boundaries of the oplog
	oplogBoundaries, err := checkpoint.GetReplicasetOplogBoundaries()
	if err != nil {
		log.Fatal("Error computing the last checkpoint: ", err)
	}

	// Replicate the collections
	for db, cols := range dbAndCollections {

		// Replicate the collections
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
				// Decrement the counter when the goroutine completes.
				defer wg.Done()
				replErr = replicateCollection(context.Background(), db, collection)
			}()
		}

		// Wait for the collections to finish
		wg.Wait()
		log.InfoWithFields("Finished full replication for database", log.Fields{
			"database": db,
		})

		if replErr != nil {
			log.Fatal("Error replicating the collections: ", replErr)
		}

		// Compute the smallest timestamp from all the databases
		//var smallestMostRecentCheckpoint primitive.Timestamp = MongoTimestampMax
		// for _, val := range ckptMap {
		// 	if CompareTimestamps(smallestMostRecentCheckpoint, val.Newest) > 0 {
		// 		smallestMostRecentCheckpoint = val.Newest
		// 	}
		// }

		log.Info("OPLog boundaries: ", oplogBoundaries)
		log.InfoWithFields("OPLog dates:", log.Fields{
			"oldest": time.Unix(int64(oplogBoundaries.Oldest.T), 0),
			"newest": time.Unix(int64(oplogBoundaries.Newest.T), 0),
		})

		// As the full replication is finished, we can save the checkpoint
		checkpointManager.SetCheckpoint(ctx, oplogBoundaries.Newest, true)

		// ckptMap = map[string]utils.TimestampNode{
		// 	coordinator.MongoS.ReplicaName: {
		// 		Newest: smallestNew,
		// 	},
		// }

		// LOG.Info("try to set checkpoint with map[%v]", ckptMap)
		// if err := docsyncer.Checkpoint(ckptMap); err != nil {
		// 	return err
		// }
	}
}

func replicateCollection(ctx context.Context, database string, collection string) error {

	writer := NewDocumentWriter(database, collection, mong.Registry.GetTarget())
	reader := NewDocumentReader(database, collection, mong.Registry.GetSource(),
		config.Current.Repl.Full.BatchSize, writer)

	// Keep track of the progress for reporting
	progress := NewSyncProgress(database, collection)
	writer.SetProgress(progress)
	reader.SetProgress(progress)

	// Start the replication
	return reader.StartSync(ctx)
}
