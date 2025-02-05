package snapshot

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BulkResult struct {
	InsertedCount           int
	UpdatedCount            int
	DeletedCount            int
	SkippedOnDuplicateCount int
	ErrorCount              int
}

type ItemWriter interface {
	Insert(ctx context.Context, item *primitive.D) error
	InsertMany(ctx context.Context, items []*bson.D) (BulkResult, error)
	Update(ctx context.Context, source *primitive.D, target *primitive.D) error
	UpdateMany(ctx context.Context, items []*bson.D) (BulkResult, error)
	Delete(ctx context.Context, id primitive.ObjectID) error
	DeleteMany(ctx context.Context, ids []primitive.ObjectID) (BulkResult, error)
	WriteMany(ctx context.Context, items []*bson.D) (BulkResult, error)
}

type MongoItemWriter struct {
	Target     *mdb.MDB
	Database   string
	Collection string
}

func NewMongoWriter(target *mdb.MDB, database string, collection string) *MongoItemWriter {
	return &MongoItemWriter{
		Target:     target,
		Database:   database,
		Collection: collection,
	}
}

func (s *MongoItemWriter) Insert(ctx context.Context, item *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).InsertOne(ctx, item)
	return err
}

func (w *MongoItemWriter) InsertMany(ctx context.Context, items []*bson.D) (BulkResult, error) {

	var result BulkResult = BulkResult{}

	// if len(items) == 0 {
	// 	log.Debug("No documents to sync")
	// 	return result, nil
	// }

	// var models []mongo.WriteModel
	// for _, item := range items {
	// 	models = append(models, mongo.NewUpdateOneModel()
	//         SetF
	//     .SetDocument(item))
	// }

	// if config.Current.Logging.Level == log.DebugLevel {
	// 	log.DebugWithFields("Inserting documents",
	// 		log.Fields{
	// 			"database":   w.Database,
	// 			"collection": w.Collection,
	// 			"count":      len(models),
	// 		})
	// }

	// // Bulk write the documents
	// opts := options.BulkWrite().SetOrdered(false)
	// _, err := w.Target.Client.Database(w.Database).Collection(w.Collection).BulkWrite(nil, models, opts)

	_, err := w.upsertManyInternal(ctx, items)

	// All documents were successfully written
	if err == nil {
		result.InsertedCount = len(items)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("Bulk write failed", err)
		result.ErrorCount = len(items)
		return result, err
	}

	return result, nil
}

func (s *MongoItemWriter) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).ReplaceOne(ctx, source, target)
	return err
}

func (w *MongoItemWriter) UpdateMany(ctx context.Context, items []*bson.D) (BulkResult, error) {

	var result BulkResult = BulkResult{}

	_, err := w.upsertManyInternal(ctx, items)

	// All documents were successfully written
	if err == nil {
		result.UpdatedCount = len(items)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("Bulk write failed", err)
		result.ErrorCount = len(items)
		return result, err
	}

	return result, nil

}

func (w *MongoItemWriter) upsertManyInternal(ctx context.Context, items []*bson.D) (*mongo.BulkWriteResult, error) {
	if len(items) == 0 {
		log.Debug("No documents to sync")
		return nil, nil
	}

	var models []mongo.WriteModel
	for _, item := range items {

		var id primitive.ObjectID = GetObjectId(item)
		var filter bson.D = bson.D{{Key: "_id", Value: id}}

		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(filter).SetUpsert(true).SetUpdate(bson.D{{"$set", item}}))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		log.InfoWithFields("Synching documents",
			log.Fields{
				"database":   w.Database,
				"collection": w.Collection,
				"count":      len(models),
			})
	}

	// Bulk write the documents
	opts := options.BulkWrite().SetOrdered(false)
	result, err := w.Target.Client.Database(w.Database).Collection(w.Collection).BulkWrite(nil, models, opts)
	return result, err
}

func (s *MongoItemWriter) Delete(ctx context.Context, id primitive.ObjectID) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).DeleteOne(ctx,
		bson.D{{Key: "_id", Value: id}})
	return err
}

func (w *MongoItemWriter) DeleteMany(ctx context.Context, ids []primitive.ObjectID) (BulkResult, error) {

	var result BulkResult = BulkResult{}

	if len(ids) == 0 {
		log.Debug("No documents to sync")
		return result, nil
	}

	var models []mongo.WriteModel
	for _, id := range ids {
		var filter bson.D = bson.D{{Key: "_id", Value: id}}
		models = append(models, mongo.NewDeleteOneModel().SetFilter(filter))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		log.DebugWithFields("Deleting documents",
			log.Fields{
				"database":   w.Database,
				"collection": w.Collection,
				"count":      len(models),
			})
	}

	// Bulk write the documents
	opts := options.BulkWrite().SetOrdered(false)
	_, err := w.Target.Client.Database(w.Database).Collection(w.Collection).BulkWrite(nil, models, opts)

	// All documents were successfully written
	if err == nil {
		result.DeletedCount = len(models)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("Bulk write failed", err)
		result.ErrorCount = len(models)
		return result, err
	}

	return result, nil
}

// Sync the documents to the target
func (r *MongoItemWriter) WriteMany(ctx context.Context, items []*bson.D) (BulkResult, error) {

	var result BulkResult = BulkResult{}

	if len(items) == 0 {
		log.Debug("No documents to sync")
		return result, nil
	}

	var models []mongo.WriteModel
	for _, item := range items {
		models = append(models, mongo.NewInsertOneModel().SetDocument(item))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		log.DebugWithFields("Synching documents",
			log.Fields{
				"database":   r.Database,
				"collection": r.Collection,
				"count":      len(items),
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
				dupDocument := *items[wError.Index]
				var updateFilter bson.D
				updateFilterBool := false

				var docData bson.D
				for _, bsonE := range docData {
					if bsonE.Key == "_id" {
						updateFilter = bson.D{bsonE}
						updateFilterBool = true
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
						"id":         GetObjectId(items[wError.Index])})
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
