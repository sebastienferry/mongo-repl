package snapshot

import (
	"bytes"
	"context"
	"errors"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DeltaReplication struct {
}

func SynchronizeCollection(ctx context.Context, database string, collection string) error {
	// Take the X first elements from source and destination

	startId := primitive.ObjectID{}

	const batchSize = 1000
	var batch = 1

	// Loop until there are no more items to sync
	for {

		log.InfoWithFields("Snapshot execution", log.Fields{
			"batch":      batch,
			"database":   database,
			"collection": collection,
		})

		// Read from source
		source, err := readItems(ctx, batchSize, mdb.Registry.GetSource(), startId, database, collection)
		if err != nil {
			log.Error("Error reading source items: ", err)
			return err
		}

		// Read from target
		target, err := readItems(ctx, batchSize, mdb.Registry.GetTarget(), startId, database, collection)
		if err != nil {
			log.Error("Error reading target items: ", err)
			return err
		}

		startId = source.HighestId

		if source.Count == 0 && target.Count == 0 {
			log.Info("No more items to sync")
			break
		}

		// Compare the two slices : source and target
		// Add missing items to target and remove extra items from target
		err = compareAndSync(ctx, source, target, NewMongoSynchronizer(mdb.Registry.GetTarget(), database, collection))
		if err != nil {
			log.Error("Error comparing and syncing items: ", err)
			return err
		}

		batch++
	}
	return nil
}

func compareAndSync(ctx context.Context, source ItemsResult, target ItemsResult, synchronizer Synchronizer) error {

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

	for sourceIndex < source.Count || targetIndex < target.Count {

		if sourceIndex < source.Count {
			sourceId, ok = getObjectId(source.Items[sourceIndex])
			if !ok {
				log.Error("Error getting source ID")
				return errors.New("Error getting source ID")
			}
		}

		if targetIndex < target.Count {
			targetId, ok = getObjectId(target.Items[targetIndex])
			if !ok {
				log.Error("Error getting target ID")
				return errors.New("Error getting target ID")
			}
		}

		// Compare the two IDs
		compare := bytes.Compare(sourceId[0:12], targetId[0:12])

		// The two IDs are the same. Update the target item
		if compare == 0 {

			log.Info("Updating document: ", source.Items[sourceIndex])
			err := synchronizer.Update(ctx, target.Items[targetIndex], source.Items[sourceIndex])
			if err != nil {
				log.Error("Error updating document: ", err)
				return err
			}

			sourceIndex++
			targetIndex++

		} else if compare > 0 {

			if targetIndex >= target.Count {
				// The source ID is lower than the target ID
				// Add the source item to the target
				err := synchronizer.Insert(ctx, source.Items[sourceIndex])
				log.Info("Inserting document: ", source.Items[sourceIndex])
				if err != nil {
					log.Error("Error inserting document: ", err)
					return err
				}

				sourceIndex++
			} else {

				// The source ID is bigger than the target ID
				// Remove the target item
				log.Info("Deleting document: ", target.Items[targetIndex])
				err := synchronizer.Delete(ctx, target.Items[targetIndex])
				if err != nil {
					log.Error("Error deleting document: ", err)
					return err
				}

				targetIndex++
			}

		} else {

			if sourceIndex >= source.Count {

				// The source ID is bigger than the target ID
				// Remove the target item
				log.Info("Deleting document: ", target.Items[targetIndex])
				err := synchronizer.Delete(ctx, target.Items[targetIndex])
				if err != nil {
					log.Error("Error deleting document: ", err)
					return err
				}

				targetIndex++

			} else {

				// The source ID is lower than the target ID
				// Add the source item to the target
				err := synchronizer.Insert(ctx, source.Items[sourceIndex])
				log.Info("Inserting document: ", source.Items[sourceIndex])
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

// Reads a batch of items from the database starting with the next ID after the `first`
// and sorted ascendingly by ID
func readItems(ctx context.Context, batchSize int, db *mdb.MDB,
	first primitive.ObjectID, database string, collection string) (ItemsResult, error) {

	// Initialize a result
	result := ItemsResult{
		Items:     make([]*bson.D, 0, batchSize),
		HighestId: first,
		Count:     0,
	}

	// Prepare the find statement
	findOptions := new(options.FindOptions)
	findOptions.SetSort(map[string]interface{}{
		"_id": 1,
	})
	findOptions.SetLimit(int64(batchSize))

	// Filter the documents
	filter := bson.D{{
		Key: "_id", Value: bson.D{{
			Key: "$gt", Value: first,
		}},
	}}

	// Read the documents
	cur, err := db.Client.Database(database).Collection(collection).Find(ctx, filter, findOptions)
	if err != nil {
		return result, err
	}

	// Prepare a buffer to store documents to sync
	for cur.Next(ctx) {

		if err := cur.Err(); err != nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return result, err
		}

		var item *bson.D = &bson.D{}
		err := cur.Decode(item)

		if err != nil || item == nil {
			log.Error("Error reading document: ", err)
			cur.Close(ctx)
			return result, err
		}

		// Update the highest ID
		oid, ok := getObjectId(item)
		if ok {
			// Note : Comparison using timestamps is not reliable
			if bytes.Compare(oid[0:12], result.HighestId[0:12]) > 0 {
				result.HighestId = oid
			}
		}

		result.Items = append(result.Items, item)
		result.Count++
	}
	return result, nil
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
