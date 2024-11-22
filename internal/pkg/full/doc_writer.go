package full

import (
	"fmt"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
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
	Target *mong.Mong
	// Progression state
	Progress *SyncProgress
}

func NewDocumentWriter(database string, collection string, target *mong.Mong) *DocumentWriter {
	return &DocumentWriter{
		Database:   database,
		Collection: collection,
		Target:     target,
	}
}

// Sync the documents to the target
func (r *DocumentWriter) WriteDocuments(docs []*bson.Raw) error {

	if len(docs) == 0 {
		log.Debug("No documents to sync")
		return nil
	}

	var models []mongo.WriteModel
	for _, doc := range docs {
		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	// qps limit if enable
	// if exec.syncer.qos.Limit > 0 {
	// 	exec.syncer.qos.FetchBucket()
	// }

	if config.Current.Logging.Level == log.DebugRunes {
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

	opts := options.BulkWrite().SetOrdered(false)
	_, err := mong.Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, models, opts)

	var successed int

	if err == nil {
		successed = len(models)
	} else {
		if _, ok := err.(mongo.BulkWriteException); !ok {
			log.Error("Bulk write failed", err)
			return err
		}

		var updateModels []mongo.WriteModel
		for _, wError := range (err.(mongo.BulkWriteException)).WriteErrors {
			if mong.IsDuplicateKeyError(wError) {

				if !config.Current.Repl.Full.UpdateOnDuplicate {
					metrics.FullSyncErrorTotal.WithLabelValues(r.Database, r.Collection, "duplicate").Add(float64(len(models)))
					break
				} else {
					log.WarnWithFields("Insert of documents failed, attempting to update them",
						log.Fields{
							"length":     len(models),
							"collection": r.Collection,
						})

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
						break
					}
					updateModels = append(updateModels, mongo.NewUpdateOneModel().
						SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))
				}

			} else {
				log.Error("Bulk write failed", err)
				metrics.FullSyncErrorTotal.WithLabelValues(r.Database, r.Collection, "bulk").Inc()
				return err
			}
		}

		if len(updateModels) != 0 {
			opts := options.BulkWrite().SetOrdered(false)
			_, err := mong.Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, updateModels, opts)
			if err != nil {
				return fmt.Errorf("bulk run updateForInsert failed[%v]", err)
			}
			successed = len(updateModels)
			log.DebugWithFields("Update on duplicate successed",
				log.Fields{
					"length":     len(updateModels),
					"collection": r.Collection,
				})

		} else {
			metrics.FullSyncErrorTotal.WithLabelValues(r.Database, r.Collection, "bulk").Add(float64(len(updateModels)))
		}
	}

	// Update metrics
	r.Progress.Increment(successed)
	metrics.FullSyncWriteCounter.WithLabelValues(r.Database, r.Collection).Add(float64(successed))
	metrics.FullSyncProgressGauge.WithLabelValues(r.Database, r.Collection).Set(r.Progress.Progress())
	return nil
}

// Set the total count of documents to sync
func (r *DocumentWriter) SetProgress(progress *SyncProgress) {
	r.Progress = progress
}
