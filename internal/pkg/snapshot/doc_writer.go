package snapshot

import (
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DocumentWriter struct {
	// The database name
	Database string
	// The name of the collection
	Collection string
	// The source database
	Target *mdb.MDB
	// Progression state
	Progress *SyncProgress
}

func NewDocumentWriter(database string, collection string, target *mdb.MDB) *DocumentWriter {
	return &DocumentWriter{
		Database:   database,
		Collection: collection,
		Target:     target,
	}
}

type WriteResult struct {
	InsertedCount           int
	UpdatedCount            int
	SkippedOnDuplicateCount int
	ErrorCount              int
}

// Sync the documents to the target
func (r *DocumentWriter) WriteDocuments(docs []*bson.Raw) (WriteResult, error) {

	var result WriteResult = WriteResult{}

	if len(docs) == 0 {
		log.Debug("No documents to sync")
		return result, nil
	}

	var models []mongo.WriteModel
	for _, doc := range docs {
		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		var docBeg, docEnd bson.M
		bson.Unmarshal(*docs[0], &docBeg)
		bson.Unmarshal(*docs[len(docs)-1], &docEnd)
		log.DebugWithFields("Synching documents",
			log.Fields{
				"database":   r.Database,
				"collection": r.Collection,
				"count":      len(docs),
			})
	}

	// Bulk write the documents
	opts := options.BulkWrite().SetOrdered(false)
	_, err := mdb.Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, models, opts)

	// All documents were successfully written
	if err == nil {
		result.InsertedCount = len(models)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("Bulk write failed", err)
		result.ErrorCount = len(models)
		return result, err
	}

	// Handle bulk-write errors and specifically duplicate key errors
	// Here we loop over all the errors and try to prepare a bull update
	// for the documents that failed to insert
	var updateModels []mongo.WriteModel
	for _, wError := range (err.(mongo.BulkWriteException)).WriteErrors {

		if mdb.IsDuplicateKeyError(wError) {

			if config.Current.Repl.Full.UpdateOnDuplicate {

				log.WarnWithFields("Insert of documents failed, attempting to update them",
					log.Fields{
						"length":     len(models),
						"collection": r.Collection,
					})

				// Prepare the update model
				dupDocument := *docs[wError.Index]
				var updateFilter bson.D
				updateFilterBool := false

				var docData bson.D
				if err := bson.Unmarshal(dupDocument, &docData); err == nil {
					for _, bsonE := range docData {
						if bsonE.Key == "_id" {
							updateFilter = bson.D{bsonE}
							updateFilterBool = true
						}
					}
				}

				if !updateFilterBool {
					log.Error("Duplicate key error, can't get _id from document", wError)
					continue
				}

				updateModels = append(updateModels, mongo.NewUpdateOneModel().
					SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))

			} else {
				result.SkippedOnDuplicateCount++

				if config.Current.Logging.Level == log.DebugLevel {
					log.ErrorWithFields("Skip duplicate", log.Fields{
						"index":      wError.Index,
						"databse":    r.Database,
						"collection": r.Collection,
						"id":         mdb.ExtractId(*docs[wError.Index]).Value})
				}
			}
		} else {
			result.ErrorCount++
			log.Error("Bulk write error with unhandled case", err)
		}
	}

	if len(updateModels) != 0 {
		opts := options.BulkWrite().SetOrdered(false)
		_, err := mdb.Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, updateModels, opts)
		if err != nil {
			result.ErrorCount = len(updateModels)
			return result, err
		}
		result.UpdatedCount = len(updateModels)
		log.DebugWithFields("Update on duplicate successed",
			log.Fields{
				"length":     len(updateModels),
				"collection": r.Collection,
			})

	}

	return result, nil
}

// Set the total count of documents to sync
func (r *DocumentWriter) SetProgress(progress *SyncProgress) {
	r.Progress = progress
}
