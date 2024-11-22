package full

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
)

func StartFullReplication(ctx context.Context) {
	for _, db := range config.Current.Repl.Databases {

		//TODO Ensure the database exist in the source

		// Get the list of collections to replicate for the database
		collections, err := mong.ListCollections(ctx, db, mong.Registry.GetSource())
		if err != nil {
			log.Fatal("Error getting the list of collections to replicate: ", err)
		}

		// Replicate the collections
		for _, collection := range collections {

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
			replicateCollection(context.Background(), db, collection)
		}
	}
}

func replicateCollection(ctx context.Context, database string, collection string) {

	// TODO Create a channel to sync between the reader and the writer ?

	// At the moment, I am not sure if I should use a channel to sync between the reader and the writer
	// So I will just pass the writer to the reader. This way, the reader can write the documents directly.
	writer := NewDocumentWriter(database, collection, mong.Registry.GetTarget())
	reader := NewDocumentReader(database, collection, mong.Registry.GetSource(),
		config.Current.Repl.Full.BatchSize, writer)

	// Keep track of the progress for reporting
	progress := NewSyncProgress(database, collection)
	writer.SetProgress(progress)
	reader.SetProgress(progress)

	// Start the replication in a goroutine
	go reader.StartSync(ctx)
}
