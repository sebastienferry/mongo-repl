package snapshot

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/sebastienferry/mongo-repl/internal/pkg/interfaces"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Holds the information to replicate a collection.
// Internally, it uses two ItemReader to read from the source and target databases
// and an ItemWriter to write to the target database.
type DeltaReplication struct {
	SourceReader interfaces.ItemReader
	TargetReader interfaces.ItemReader
	TargetWriter interfaces.ItemWriter
	Database     string
	Collection   string
	Initial      bool
	BatchSize    int

	// State variables
	currentBatch  int
	firstId       primitive.ObjectID
	itemsToInsert []*bson.D
	itemsToUpdate []*bson.D
	itemsToDelete []primitive.ObjectID
}

// Creates a new DeltaReplication object.
func NewDeltaReplication(sourceReader interfaces.ItemReader, targetReader interfaces.ItemReader, targetWriter interfaces.ItemWriter,
	database string, collection string, initial bool, batchSize int) *DeltaReplication {
	return &DeltaReplication{
		SourceReader: sourceReader,
		TargetReader: targetReader,
		TargetWriter: targetWriter,
		Database:     database,
		Collection:   collection,
		Initial:      initial,
		BatchSize:    batchSize,
	}
}

// Synchronize the collection.
func (r *DeltaReplication) SynchronizeCollection(ctx context.Context) error {

	r.currentBatch = 1
	r.firstId = primitive.ObjectID{}

	// Prepare to track the replication progress
	progress := NewSyncProgress(r.Database, r.Collection)
	total, err := r.SourceReader.Count(ctx)
	if err != nil {
		log.ErrorWithFields("error getting source count: ", log.Fields{
			"database":   r.Database,
			"collection": r.Collection,
			"err":        err})
		return err
	}
	progress.SetTotal(total)

	// Loop until there are no more items to sync
	for {

		// Read from source
		source, err := r.SourceReader.ReadItems(ctx, r.BatchSize, r.firstId)
		if err != nil {
			log.Error("error reading source items: ", err)
			return err
		}

		lastId := primitive.ObjectID{}
		if len(source) > 0 {
			lastId = GetObjectId(source[len(source)-1])
		}

		// Read from target all the items between startId and endId
		// but only for non initial snapshot (meaning db is not empty)
		// otherwise can simply read source and insert all items.
		var target []*bson.D
		if !r.Initial {
			target, err = r.TargetReader.ReadItems(ctx, r.BatchSize, r.firstId, lastId)
			if err != nil {
				log.Error("error reading target items: ", err)
				return err
			}
		}

		if len(source) == 0 && len(target) == 0 {
			log.InfoWithFields("no more items to sync", log.Fields{
				"progress":   progress.Progress(),
				"database":   r.Database,
				"collection": r.Collection})
			break
		}

		log.InfoWithFields("snapshot execution", log.Fields{
			"progress":   progress.Progress(),
			"batch":      r.currentBatch,
			"database":   r.Database,
			"collection": r.Collection,
		})

		// Compare the two slices : source and target
		// Compute the list of items to insert, update and delete
		err = r.computeDelta(source, target)
		if err != nil {
			log.Error("error comparing and syncing items: ", err)
			return err
		}

		// Insert, update and delete the items
		if len(r.itemsToInsert) > 0 {
			inserted, err := r.TargetWriter.InsertMany(ctx, r.itemsToInsert)
			if err != nil {
				log.Error("error inserting documents: ", err)
				return err
			}

			// Update the progress and metrics
			progress.Increment(inserted.InsertedCount)
			metrics.SnapshotWriteCounter.WithLabelValues(r.Database, r.Collection, metrics.InsertOp).Add(float64(inserted.InsertedCount))
			metrics.SnapshotErrorTotal.WithLabelValues(r.Database, r.Collection, metrics.InsertOp).Add(float64(inserted.ErrorCount))
		}

		if len(r.itemsToUpdate) > 0 {
			updated, err := r.TargetWriter.UpdateMany(ctx, r.itemsToUpdate)
			if err != nil {
				log.Error("error updating documents: ", err)
				return err
			}

			// Update the progress and metrics
			progress.Increment(updated.UpdatedCount)
			metrics.SnapshotWriteCounter.WithLabelValues(r.Database, r.Collection, metrics.UpdateOp).Add(float64(updated.UpdatedCount))
			metrics.SnapshotErrorTotal.WithLabelValues(r.Database, r.Collection, metrics.UpdateOp).Add(float64(updated.ErrorCount))
		}

		if len(r.itemsToDelete) > 0 {
			deleted, err := r.TargetWriter.DeleteMany(ctx, r.itemsToDelete)
			if err != nil {
				log.Error("error deleting documents: ", err)
				return err
			}

			// Update the metrics, but not the progress.
			// Do not count the items to delete in the progress, only upsert to avoid goind over 100%
			metrics.SnapshotWriteCounter.WithLabelValues(r.Database, r.Collection, metrics.DeleteOp).Add(float64(deleted.DeletedCount))
			metrics.SnapshotErrorTotal.WithLabelValues(r.Database, r.Collection, metrics.DeleteOp).Add(float64(deleted.ErrorCount))
		}

		// Do not count the items to delete in the progress, only upsert to avoid goind over 100%
		metrics.SnapshotProgressGauge.WithLabelValues(r.Database, r.Collection).Set(progress.Progress())
		r.currentBatch++
	}
	return nil
}

func (r *DeltaReplication) computeDelta(source []*bson.D, target []*bson.D) error {

	// Compare the two slices : source and target
	// Nota bene: The slices are sorted by ID
	// Add missing items to target
	// Update the target with the source items
	// Remove extra items from target

	var sourceId, lastSourceId primitive.ObjectID
	var targetId, lastTargetId primitive.ObjectID
	var ok bool

	sourceCount := len(source)
	targetCount := len(target)

	r.itemsToInsert = make([]*bson.D, 0, r.BatchSize)
	r.itemsToUpdate = make([]*bson.D, 0, r.BatchSize)
	r.itemsToDelete = make([]primitive.ObjectID, 0, r.BatchSize)

	// we loop until we reach the end of at least one slice.
	var lowest int = (int)(math.Min(float64(sourceCount), float64(targetCount)))
	var sourceIndex int
	var targetIndex int
	for sourceIndex < lowest || targetIndex < lowest {

		// we did not reach the end of the source slice
		if sourceIndex < lowest {
			sourceId, ok = TryGetObjectId(source[sourceIndex])
			if !ok {
				log.Error("error getting source ID")
				return errors.New("error getting source ID")
			}
		} else {
			sourceId = primitive.ObjectID{}
		}

		// we did not reach the end of the target slice
		if targetIndex < lowest {
			targetId, ok = TryGetObjectId(target[targetIndex])
			if !ok {
				log.Error("error getting target ID")
				return errors.New("error getting target ID")
			}
		} else {
			targetId = primitive.ObjectID{}
		}

		var compare int
		// Compare the two IDs
		if !sourceId.IsZero() && !targetId.IsZero() {
			compare = bytes.Compare(sourceId[:], targetId[:])
		} else if sourceId.IsZero() && !targetId.IsZero() {
			compare = 1
		} else if !sourceId.IsZero() && targetId.IsZero() {
			compare = -1
		} else {
			compare = 0
		}

		// The two IDs are the same. Update the target item
		if compare == 0 {

			// The source ID is equal to the target ID
			// ==> Update the target item
			//log.Info("update document: ", source[sourceIndex])
			r.itemsToUpdate = append(r.itemsToUpdate, source[sourceIndex])
			lastSourceId = sourceId
			sourceIndex++
			targetIndex++
		} else if compare > 0 {

			// The source ID is bigger than the target ID
			// ==> Remove the target item
			//log.Info("remove document: ", target[targetIndex])
			idToDelete, ok := TryGetObjectId(target[targetIndex])
			if !ok {
				log.Error("error getting target ID")
				return errors.New("error getting target ID")
			}

			r.itemsToDelete = append(r.itemsToDelete, idToDelete)
			lastTargetId = targetId
			targetIndex++
		} else {

			// The source ID is lower than the target ID
			// ==> Insert the source item to the target
			//log.Info("insert document: ", source[sourceIndex])
			r.itemsToInsert = append(r.itemsToInsert, source[sourceIndex])
			lastSourceId = sourceId
			sourceIndex++
		}
	}

	// we reach the end of either the source or the target slice
	// we need to handle the remaining items
	if sourceCount > targetCount {

		// The source slice is bigger than the target slice
		// ==> Add the missing items
		for index := targetCount; index < sourceCount; index++ {
			// log.Debug("insert document (ramasse miettes): ", source[index])
			r.itemsToInsert = append(r.itemsToInsert, source[index])
		}

		r.firstId = GetObjectId(r.itemsToInsert[len(r.itemsToInsert)-1])

	} else if sourceCount < targetCount {

		// The target slice is bigger than the source slice
		// ==> Remove the extra items
		for index := sourceCount; index < targetCount; index++ {
			log.Info("delete document (ramasse miettes): ", target[index])
			idToDelete, ok := TryGetObjectId(target[index])
			if !ok {
				log.Error("error getting target ID")
				return errors.New("error getting target ID")
			}
			r.itemsToDelete = append(r.itemsToDelete, idToDelete)
		}

		r.firstId = r.itemsToDelete[len(r.itemsToDelete)-1]
	}

	if !lastSourceId.IsZero() && !lastTargetId.IsZero() {
		compare := bytes.Compare(lastSourceId[:], lastTargetId[:])
		if compare >= 0 {
			r.firstId = lastTargetId
		} else {
			r.firstId = lastSourceId
		}
	} else if lastSourceId.IsZero() && !lastTargetId.IsZero() {
		r.firstId = lastTargetId
	} else if !lastSourceId.IsZero() && lastTargetId.IsZero() {
		r.firstId = lastSourceId
	}

	return nil
}

func TryGetObjectId(document *bson.D) (primitive.ObjectID, bool) {
	for _, elem := range *document {
		if elem.Key == "_id" {
			if oid, ok := elem.Value.(primitive.ObjectID); ok {
				return oid, true
			}
		}
	}
	return primitive.ObjectID{}, false
}

func GetObjectId(document *bson.D) primitive.ObjectID {
	for _, elem := range *document {
		if elem.Key == "_id" {
			if oid, ok := elem.Value.(primitive.ObjectID); ok {
				return oid
			}
		}
	}
	return primitive.ObjectID{}
}
