package snapshot

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DeltaReplication struct {
	SourceReader ItemReader
	TargetReader ItemReader
	TargetWriter ItemWriter
	Database     string
	Collection   string
	Initial      bool
	BatchSize    int

	// The current batch
	currentBatch  int
	firstId       primitive.ObjectID
	itemsToInsert []*bson.D
	itemsToUpdate []*bson.D
	itemsToDelete []primitive.ObjectID
}

func NewDeltaReplication(sourceReader ItemReader, targetReader ItemReader, targetWriter ItemWriter,
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

func (r *DeltaReplication) SynchronizeCollection(ctx context.Context) error {

	r.currentBatch = 1
	r.firstId = primitive.ObjectID{}

	// Loop until there are no more items to sync
	for {

		// Read from source
		source, err := r.SourceReader.ReadItems(ctx, r.BatchSize, r.firstId)
		if err != nil {
			log.Error("Error reading source items: ", err)
			return err
		}

		lastId := primitive.ObjectID{}
		if len(source) > 0 {
			lastId = GetObjectId(source[len(source)-1])
		}

		//log.Info(fmt.Sprintf("IDs range: ]%s - %s]", r.firstId.String(), lastId.String()))

		// Read from target all the items between startId and endId
		target := make([]*bson.D, 0)
		if !r.Initial {
			target, err = r.TargetReader.ReadItems(ctx, r.BatchSize, r.firstId, lastId)
			if err != nil {
				log.Error("Error reading target items: ", err)
				return err
			}
		}

		if len(source) == 0 && len(target) == 0 {
			log.InfoWithFields("No more items to sync", log.Fields{
				"database":   r.Database,
				"collection": r.Collection})
			break
		}

		log.InfoWithFields("Snapshot execution", log.Fields{
			"batch":      r.currentBatch,
			"database":   r.Database,
			"collection": r.Collection,
		})

		// Compare the two slices : source and target
		// Compute the list of items to insert, update and delete
		err = r.computeDelta(ctx, source, target)
		if err != nil {
			log.Error("Error comparing and syncing items: ", err)
			return err
		}

		// Insert, update and delete the items
		if len(r.itemsToInsert) > 0 {
			_, err = r.TargetWriter.InsertMany(ctx, r.itemsToInsert)
			if err != nil {
				log.Error("Error inserting documents: ", err)
				return err
			}
		}

		if len(r.itemsToUpdate) > 0 {
			_, err = r.TargetWriter.UpdateMany(ctx, r.itemsToUpdate)
			if err != nil {
				log.Error("Error updating documents: ", err)
				return err
			}
		}

		if len(r.itemsToDelete) > 0 {
			_, err = r.TargetWriter.DeleteMany(ctx, r.itemsToDelete)
			if err != nil {
				log.Error("Error deleting documents: ", err)
				return err
			}
		}
		r.currentBatch++
	}
	return nil
}

func (r *DeltaReplication) computeDelta(ctx context.Context, source []*bson.D, target []*bson.D) error {

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

	// We loop until we reach the end of at least one slice.
	var lowest int = (int)(math.Min(float64(sourceCount), float64(targetCount)))
	var sourceIndex int
	var targetIndex int
	for sourceIndex < lowest || targetIndex < lowest {

		// We did not reach the end of the source slice
		if sourceIndex < lowest {
			sourceId, ok = TryGetObjectId(source[sourceIndex])
			if !ok {
				log.Error("Error getting source ID")
				return errors.New("Error getting source ID")
			}
		} else {
			sourceId = primitive.ObjectID{}
		}

		// We did not reach the end of the target slice
		if targetIndex < lowest {
			targetId, ok = TryGetObjectId(target[targetIndex])
			if !ok {
				log.Error("Error getting target ID")
				return errors.New("Error getting target ID")
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
			//log.Info("Update document: ", source[sourceIndex])
			r.itemsToUpdate = append(r.itemsToUpdate, source[sourceIndex])
			lastSourceId = sourceId
			sourceIndex++
			targetIndex++
		} else if compare > 0 {

			// The source ID is bigger than the target ID
			// ==> Remove the target item
			//log.Info("Remove document: ", target[targetIndex])
			idToDelete, ok := TryGetObjectId(target[targetIndex])
			if !ok {
				log.Error("Error getting target ID")
				return errors.New("Error getting target ID")
			}

			r.itemsToDelete = append(r.itemsToDelete, idToDelete)
			lastTargetId = targetId
			targetIndex++
		} else {

			// The source ID is lower than the target ID
			// ==> Insert the source item to the target
			//log.Info("Insert document: ", source[sourceIndex])
			r.itemsToInsert = append(r.itemsToInsert, source[sourceIndex])
			lastSourceId = sourceId
			sourceIndex++
		}
	}

	// We reach the end of either the source or the target slice
	// We need to handle the remaining items
	if sourceCount > targetCount {

		// The source slice is bigger than the target slice
		// ==> Add the missing items
		for index := targetCount; index < sourceCount; index++ {
			// log.Debug("Insert document (ramasse miettes): ", source[index])
			r.itemsToInsert = append(r.itemsToInsert, source[index])
		}

		r.firstId = GetObjectId(r.itemsToInsert[len(r.itemsToInsert)-1])

	} else if sourceCount < targetCount {

		// The target slice is bigger than the source slice
		// ==> Remove the extra items
		for index := sourceCount; index < targetCount; index++ {
			log.Info("Delete document (ramasse miettes): ", target[index])
			idToDelete, ok := TryGetObjectId(target[index])
			if !ok {
				log.Error("Error getting target ID")
				return errors.New("Error getting target ID")
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
