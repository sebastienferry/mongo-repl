package stats

import (
	"context"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
)

type CollectionStats struct {
	collections map[string][]string
	done        chan bool
}

func NewCollectionStats(initialConnections map[string][]string) *CollectionStats {
	return &CollectionStats{
		collections: initialConnections,
		done:        make(chan bool),
	}
}

func (c *CollectionStats) StartCollectionStats(ctx context.Context) {

	// Start the collection stats observation
	go func() {
		iter := 0
		for {
			select {
			case <-time.After(30 * time.Second):
			case <-c.done:
				return
			}

			for db, collections := range c.collections {
				c.getCollectionStats(db, collections)
			}

			// Every 4 iterations, we refresh the list of collections
			// from the source database.
			if iter%4 == 0 {
				cols, err := mdb.GetCollections(ctx, config.Current.Repl.Databases)
				if err != nil {
					log.Error("error getting the list of collections to replicate: ", err)
					continue
				}
				c.collections = cols
			}

			iter++
		}
	}()
}

func (c *CollectionStats) getCollectionStats(db string, collections []string) {
	for _, collection := range collections {

		// GEt the stats for the source and target
		count, err := mdb.GetStatsByCollection(mdb.Registry.GetSource(), db, collection)
		if err != nil {
			continue
		}

		metrics.MongoReplSourceTotalDocumentCount.WithLabelValues("source", db, collection).Set(float64(count))

		count, err = mdb.GetStatsByCollection(mdb.Registry.GetTarget(), db, collection)
		if err != nil {
			continue
		}

		metrics.MongoReplSourceTotalDocumentCount.WithLabelValues("target", db, collection).Set(float64(count))
	}
}

func (c *CollectionStats) StopCollectionStats() {
	c.done <- true
}
