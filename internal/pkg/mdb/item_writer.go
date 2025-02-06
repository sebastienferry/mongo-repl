package mdb

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/interfaces"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoItemWriter struct {
	Target     *MDB
	Database   string
	Collection string
}

func NewMongoWriter(target *MDB, database string, collection string) *MongoItemWriter {
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

func (w *MongoItemWriter) InsertMany(ctx context.Context, items []*bson.D) (interfaces.BulkResult, error) {

	var result interfaces.BulkResult = interfaces.BulkResult{}

	_, err := w.upsertManyInternal(ctx, items)

	// All documents were successfully written
	if err == nil {
		result.InsertedCount = len(items)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("bulk write failed", err)
		result.ErrorCount = len(items)
		return result, err
	}

	return result, nil
}

func (s *MongoItemWriter) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).ReplaceOne(ctx, source, target)
	return err
}

func (w *MongoItemWriter) UpdateMany(ctx context.Context, items []*bson.D) (interfaces.BulkResult, error) {

	var result interfaces.BulkResult = interfaces.BulkResult{}

	_, err := w.upsertManyInternal(ctx, items)

	// All documents were successfully written
	if err == nil {
		result.UpdatedCount = len(items)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("bulk write failed", err)
		result.ErrorCount = len(items)
		return result, err
	}

	return result, nil

}

func (w *MongoItemWriter) upsertManyInternal(ctx context.Context, items []*bson.D) (*mongo.BulkWriteResult, error) {
	if len(items) == 0 {
		log.Debug("no documents to sync")
		return nil, nil
	}

	var models []mongo.WriteModel
	for _, item := range items {

		var id primitive.ObjectID = GetObjectId(*item)
		var filter bson.D = bson.D{{Key: "_id", Value: id}}

		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(filter).SetUpsert(true).SetUpdate(bson.D{{"$set", item}}))
	}

	// Bulk write the documents
	opts := options.BulkWrite().SetOrdered(false)
	result, err := w.Target.Client.Database(w.Database).Collection(w.Collection).BulkWrite(ctx, models, opts)
	return result, err
}

func (s *MongoItemWriter) Delete(ctx context.Context, id primitive.ObjectID) error {
	_, err := s.Target.Client.Database(s.Database).Collection(s.Collection).DeleteOne(ctx,
		bson.D{{Key: "_id", Value: id}})
	return err
}

func (w *MongoItemWriter) DeleteMany(ctx context.Context, ids []primitive.ObjectID) (interfaces.BulkResult, error) {

	var result interfaces.BulkResult = interfaces.BulkResult{}

	if len(ids) == 0 {
		log.Debug("no documents to sync")
		return result, nil
	}

	var models []mongo.WriteModel
	for _, id := range ids {
		var filter bson.D = bson.D{{Key: "_id", Value: id}}
		models = append(models, mongo.NewDeleteOneModel().SetFilter(filter))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		log.DebugWithFields("deleting documents",
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
		log.Error("bulk write failed", err)
		result.ErrorCount = len(models)
		return result, err
	}

	return result, nil
}

// Sync the documents to the target
// deprecated
func (r *MongoItemWriter) WriteMany(ctx context.Context, items []*bson.D) (interfaces.BulkResult, error) {

	var result interfaces.BulkResult = interfaces.BulkResult{}

	if len(items) == 0 {
		log.Debug("no documents to sync")
		return result, nil
	}

	var models []mongo.WriteModel
	for _, item := range items {
		models = append(models, mongo.NewInsertOneModel().SetDocument(item))
	}

	if config.Current.Logging.Level == log.DebugLevel {
		log.DebugWithFields("synching documents",
			log.Fields{
				"database":   r.Database,
				"collection": r.Collection,
				"count":      len(items),
			})
	}

	// Bulk write the documents
	opts := options.BulkWrite().SetOrdered(false)
	_, err := Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, models, opts)

	// All documents were successfully written
	if err == nil {
		result.InsertedCount = len(models)
		return result, nil
	}

	// Handle non-bulk write errors
	if _, ok := err.(mongo.BulkWriteException); !ok {
		log.Error("bulk write failed", err)
		result.ErrorCount = len(models)
		return result, err
	}

	// Handle bulk-write errors and specifically duplicate key errors
	// Here we loop over all the errors and try to prepare a bull update
	// for the documents that failed to insert
	var updateModels []mongo.WriteModel
	for _, wError := range (err.(mongo.BulkWriteException)).WriteErrors {

		if IsDuplicateKeyError(wError) {

			if config.Current.Repl.Full.UpdateOnDuplicate {

				log.WarnWithFields("insert of documents failed, attempting to update them",
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
					log.Error("duplicate key error, can't get _id from document", wError)
					continue
				}

				updateModels = append(updateModels, mongo.NewUpdateOneModel().
					SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))

			} else {
				result.SkippedOnDuplicateCount++

				if config.Current.Logging.Level == log.DebugLevel {
					log.ErrorWithFields("skip duplicate", log.Fields{
						"index":      wError.Index,
						"databse":    r.Database,
						"collection": r.Collection,
						"id":         GetObjectId(*items[wError.Index])})
				}
			}
		} else {
			result.ErrorCount++
			log.Error("bulk write error with unhandled case", err)
		}
	}

	if len(updateModels) != 0 {
		opts := options.BulkWrite().SetOrdered(false)
		_, err := Registry.GetTarget().Client.Database(r.Database).Collection(r.Collection).BulkWrite(nil, updateModels, opts)
		if err != nil {
			result.ErrorCount = len(updateModels)
			return result, err
		}
		result.UpdatedCount = len(updateModels)
		log.DebugWithFields("update on duplicate successed",
			log.Fields{
				"length":     len(updateModels),
				"collection": r.Collection,
			})

	}

	return result, nil
}
