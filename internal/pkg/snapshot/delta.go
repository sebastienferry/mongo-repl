package snapshot

import (
	"bytes"
	"context"
	"errors"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DeltaReplication struct {
}

func SynchronizeCollection(ctx context.Context, batchSize int, sourceReader ItemReader, targetReader ItemReader,
	synchronizer Synchronizer, database string, collection string) error {
	// Take the X first elements from source and destination

	startId := primitive.ObjectID{}

	var batch = 1
	// Loop until there are no more items to sync
	for {

		log.InfoWithFields("Snapshot execution", log.Fields{
			"batch":      batch,
			"database":   database,
			"collection": collection,
		})

		// Read from source
		source, err := sourceReader.ReadItems(ctx, batchSize, startId)
		if err != nil {
			log.Error("Error reading source items: ", err)
			return err
		}

		// Read from target
		target, err := targetReader.ReadItems(ctx, batchSize, startId)
		if err != nil {
			log.Error("Error reading target items: ", err)
			return err
		}

		var ok bool = true
		if len(source) > 0 {
			startId, ok = getObjectId(source[len(source)-1])
			if !ok {
				log.Error("Error getting source ID: ", err)
				return err
			}
		}

		if !ok {
			log.Error("Error getting source ID: ", err)
			return err
		}

		if len(source) == 0 && len(target) == 0 {
			log.Info("No more items to sync")
			break
		}

		// Compare the two slices : source and target
		// Add missing items to target and remove extra items from target
		err = compareAndSync(ctx, source, target, synchronizer)
		if err != nil {
			log.Error("Error comparing and syncing items: ", err)
			return err
		}

		batch++
	}
	return nil
}

func compareAndSync(ctx context.Context, source []*bson.D, target []*bson.D, synchronizer Synchronizer) error {

	// Compare the two slices : source and target
	// Nota bene: The slices are sorted by ID
	// Add missing items to target
	// Update the target with the source items
	// Remove extra items from target
	sourceIndex := 0
	targetIndex := 0

	var sourceId primitive.ObjectID
	var targetId primitive.ObjectID
	var ok bool

	sourceCount := len(source)
	targetCount := len(target)

	for sourceIndex < sourceCount || targetIndex < targetCount {

		if sourceIndex < sourceCount {
			sourceId, ok = getObjectId(source[sourceIndex])
			if !ok {
				log.Error("Error getting source ID")
				return errors.New("Error getting source ID")
			}
		}

		if targetIndex < targetCount {
			targetId, ok = getObjectId(target[targetIndex])
			if !ok {
				log.Error("Error getting target ID")
				return errors.New("Error getting target ID")
			}
		}

		// Compare the two IDs
		compare := bytes.Compare(sourceId[:], targetId[:])

		// The two IDs are the same. Update the target item
		if compare == 0 {

			log.Info("Updating document: ", source[sourceIndex])
			err := synchronizer.Update(ctx, target[targetIndex], source[sourceIndex])
			if err != nil {
				log.Error("Error updating document: ", err)
				return err
			}

			sourceIndex++
			targetIndex++

		} else if compare > 0 {

			if targetIndex >= targetCount {
				err := synchronizer.Insert(ctx, source[sourceIndex])
				log.Info("Inserting document: ", source[sourceIndex])
				if err != nil {
					log.Error("Error inserting document: ", err)
					return err
				}

				sourceIndex++
			} else {

				// The source ID is bigger than the target ID
				// Remove the target item
				log.Info("Deleting document: ", target[targetIndex])

				idToDelete, ok := getObjectId(target[targetIndex])
				if !ok {
					log.Error("Error getting target ID")
					return errors.New("Error getting target ID")
				}

				err := synchronizer.Delete(ctx, idToDelete)
				if err != nil {
					log.Error("Error deleting document: ", err)
					return err
				}

				targetIndex++
			}

		} else {

			if sourceIndex >= sourceCount {

				// The source ID is bigger than the target ID
				// Remove the target item
				log.Info("Deleting document: ", target[targetIndex])

				idToDelete, ok := getObjectId(target[targetIndex])
				if !ok {
					log.Error("Error getting target ID")
					return errors.New("Error getting target ID")
				}

				err := synchronizer.Delete(ctx, idToDelete)
				if err != nil {
					log.Error("Error deleting document: ", err)
					return err
				}

				targetIndex++

			} else {

				// The source ID is lower than the target ID
				// Add the source item to the target
				err := synchronizer.Insert(ctx, source[sourceIndex])
				log.Info("Inserting document: ", source[sourceIndex])
				if err != nil {
					log.Error("Error inserting document: ", err)
					return err
				}

				sourceIndex++
			}
		}
	}

	return nil
}

func getObjectId(document *bson.D) (primitive.ObjectID, bool) {
	for _, elem := range *document {
		if elem.Key == "_id" {
			if oid, ok := elem.Value.(primitive.ObjectID); ok {
				return oid, true
			}
		}
	}
	return primitive.ObjectID{}, false
}
