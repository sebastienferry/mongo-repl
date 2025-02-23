package snapshot

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/interfaces"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DocumentReader struct {
	// The database name
	Database string
	// The name of the collection
	Collection string
	// The batch size
	BatchSize int
	// The source database
	Source *mdb.MDB
	// The document writer
	Writer *DocumentWriter
	// Progression state
	Progress *SyncProgress
}

const (
	MAX_BUFFER_BYTE_SIZE = 12 * 1024 * 1024
)

func NewDocumentReader(database string, collection string, source *mdb.MDB, batchSize int, writer *DocumentWriter) *DocumentReader {
	return &DocumentReader{
		Database:   database,
		Collection: collection,
		BatchSize:  batchSize,
		Source:     source,
		Writer:     writer,
	}
}

// Read a batch of documents from the source
func (r *DocumentReader) Replicate(ctx context.Context) error {

	log.Info("start syncing collection ", r.Collection)

	// get total count
	count, err := mdb.GetStatsByCollection(r.Source, r.Database, r.Collection)
	if err != nil {
		log.Error("error getting collection stats: ", err)
		return err
	}

	r.Progress.SetTotal(count)
	log.InfoWithFields("collection stats", log.Fields{
		"collection": r.Collection,
		"count":      count})

	findOptions := new(options.FindOptions)
	findOptions.SetSort(map[string]interface{}{
		"_id": 1,
	})
	findOptions.SetBatchSize(int32(r.BatchSize))
	findOptions.SetHint(map[string]interface{}{
		"_id": 1,
	})

	// Filter the documents
	filter := bson.D{{}}
	//filter = append(filter, bson.D{"_id", bson.D{{"$gt", r.lastId}}})

	// Read the documents
	db := r.Source.Client.Database(r.Database)
	cur, err := db.Collection(r.Collection).Find(ctx, filter, findOptions)
	if err != nil {
		return err
	}

	limit := NewQpsLimit(30000)
	limit.Reset()

	// Prepare a buffer to store documents to sync
	bufferSize := 128
	buffer := make([]*bson.Raw, 0, 128)
	bufferByteSize := 0

	for cur.Next(ctx) {

		if err := cur.Err(); err != nil {
			log.Error("error reading document: ", err)
			cur.Close(ctx)
			return err
		}

		// Get the raw document
		raw := cur.Current
		if raw == nil {
			log.Error("error reading document: ", err)
			cur.Close(ctx)
		}

		// Wait for the QPS limit
		limit.Wait()

		// Doc received
		count := len(buffer)
		limit.Incr(count)

		// Successfully read a batch of documents. Increment the counter
		metrics.SnapshotReadCounter.WithLabelValues(r.Database, r.Collection).Inc()

		if bufferByteSize+len(raw) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {

			// Send the buffer to the target
			// TODO: At the moment, I am not sure if I should use a channel to sync between the reader and the writer
			result, err := r.Writer.WriteDocuments(buffer)
			if err != nil {
				log.Error("error syncing documents: ", err)
				return err
			}

			// Update metrics
			r.ReportResult(result)

			// Reset the buffer
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		buffer = append(buffer, &raw)
		bufferByteSize += len(raw)
	}

	// Send the remaining buffer
	if len(buffer) > 0 {
		result, err := r.Writer.WriteDocuments(buffer)
		if err != nil {
			log.Error("error syncing documents: ", err)
		}

		// Update metrics
		r.ReportResult(result)
	}

	log.InfoWithFields("finished full replication for collection", log.Fields{
		"database":   r.Database,
		"collection": r.Collection,
	})

	return nil
}

// Report the result of the write operation
func (r *DocumentReader) ReportResult(result interfaces.BulkResult) {
	// TODO : Should we reflect the success rate or the progress ?
	r.Progress.Increment(result.InsertedCount + result.UpdatedCount + result.SkippedOnDuplicateCount + result.ErrorCount)
	metrics.SnapshotWriteCounter.WithLabelValues(r.Database, r.Collection, "insert").Add(float64(result.InsertedCount))
	metrics.SnapshotWriteCounter.WithLabelValues(r.Database, r.Collection, "update").Add(float64(result.UpdatedCount))
	metrics.SnapshotErrorTotal.WithLabelValues(r.Database, r.Collection, "skip").Add(float64(result.SkippedOnDuplicateCount))
	metrics.SnapshotErrorTotal.WithLabelValues(r.Database, r.Collection, "bulk").Add(float64(result.ErrorCount))
	metrics.SnapshotProgressGauge.WithLabelValues(r.Database, r.Collection).Set(r.Progress.Progress())
}

// Set the total count of documents to sync
func (r *DocumentReader) SetProgress(progress *SyncProgress) {
	r.Progress = progress
}
