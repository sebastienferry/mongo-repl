package incr

import (
	"context"
	"fmt"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	VersionMark       = "$v"
	uuidMark          = "ui"
	shardKeyupdateErr = "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1"
)

type OplogWriter struct {
	queuedLogs   chan *ChangeLog
	fullFinishTs int64
}

func NewOplogWriter(fullFinishTs int64, queuedLogs chan *ChangeLog) *OplogWriter {
	return &OplogWriter{
		queuedLogs:   queuedLogs,
		fullFinishTs: fullFinishTs,
	}
}

// Montly taken from db_writer_bulk.go
func (ow *OplogWriter) StartWriter() {

	log.Info("Starting the oplog writer")

	go func() {
		// var updates []mongo.WriteModel = make([]mongo.WriteModel, 0)
		// var startTime = time.Now()
		for l := range ow.queuedLogs {

			// Display some debug information
			debugLog(&l.ParsedLog)

			// Get the write operation
			//updates = append(updates, ow.getWriteOperation(l))

			// Handle the operation
			var opErr error = nil
			switch l.Operation {
			case "i":
				opErr = ow.handleInsert(l)
			case "u":
				opErr = ow.handleUpdate(l, true)
			case "d":
				opErr = ow.handleDelete(l)
			}

			// Check for errors
			if opErr != nil {
				log.ErrorWithFields("Error handling operation", log.Fields{
					"err": opErr,
					"op":  l.Operation,
				})
			}

			// if len(updates) > 10 {
			// 	// Bulk write
			// 	log.DebugWithFields("Bulk writing", log.Fields{"count": len(updates)})
			// }

			// // Write the updates if we have more than 10
			// // or if we have been waiting for more than 5 seconds
			// if len(updates) > 10 || time.Since(startTime) > 5*time.Second {
			// 	// Bulk write

			// 	// Reset the updates
			// 	updates = make([]mongo.WriteModel, 0)
			// }

		}
	}()
}

func (ow *OplogWriter) handleInsert(l *ChangeLog) error {

	collectionHandle := mong.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	// var upserts []*OplogRecord

	//for _, log := range oplogs {
	if _, err := collectionHandle.InsertOne(context.Background(), l.ParsedLog.Object); err != nil {

		if mongo.IsDuplicateKeyError(err) {

			// Handle upsert
			err = ow.handleUpdateOnInsert(l, true)
			return err
		} else {
			log.ErrorWithFields("Insert error", log.Fields{"err": err})
			return err
		}
	}

	//LOG.Debug("single_writer: insert %v", log.original.partialLog)
	//

	// if len(upserts) != 0 {
	// 	RecordDuplicatedOplog(sw.conn, collection, upserts)

	// 	// update on duplicated key occur
	// 	if dupUpdate {
	// 		LOG.Info("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
	// 		return sw.doUpdateOnInsert(database, collection, metadata, upserts, conf.Options.IncrSyncExecutorUpsert)
	// 	}
	// 	return nil
	// }
	return nil

}

func (sw *OplogWriter) handleUpdateOnInsert(l *ChangeLog, upsert bool) error {

	collectionHandle := mong.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	// type pair struct {
	// 	id    interface{}
	// 	data  bson.D
	// 	index int
	// }
	// var updates []*pair
	// for i, log := range oplogs {
	var update interface{} = bson.D{{"$set", l.ParsedLog.Object}}
	var id primitive.D

	if upsert && len(l.ParsedLog.DocumentKey) > 0 {
		//updates = append(updates, &pair{id: l.ParsedLog.DocumentKey, data: newObject, index: i})
		id = l.ParsedLog.DocumentKey
	} else {
		// if upsert {
		// 	LOG.Warn("doUpdateOnInsert runs upsert but lack documentKey: %v", l.ParsedLog)
		// }
		// insert must have _id
		if id := GetKey(l.ParsedLog.Object, ""); id != nil {
			//updates = append(updates, &pair{id: bson.D{{"_id", id}}, data: newObject, index: i})
			//update = bson.D{{"$set", newObject}}
			id = bson.D{{"_id", id}}
		} else {
			return fmt.Errorf("insert on duplicated update _id look up failed. %v", l.ParsedLog)
		}
	}

	//LOG.Debug("single_writer: updateOnInsert %v", l.ParsedLog)
	//}

	if upsert {
		//for _, update := range updates {

		opts := options.Update().SetUpsert(true)
		res, err := collectionHandle.UpdateOne(context.Background(), id, update, opts)
		if err != nil {
			log.Warn("upsert _id[%v] with data[%v] meets err[%v] res[%v], try to solve",
				id, update, err, res)

			// error can be ignored(insert fail & oplog is before full end)
			if mongo.IsDuplicateKeyError(err) &&
				checkpoint.ToInt64(l.ParsedLog.Timestamp) <= sw.fullFinishTs {
				return nil
			}

			log.Error("upsert _id[%v] with data[%v] failed[%v]", id, update, err)
			return err
		}
		if res != nil {
			if res.MatchedCount != 1 && res.UpsertedCount != 1 {
				return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d) upsert _id[%v] with data[%v]",
					res.MatchedCount, res.ModifiedCount, res.UpsertedCount, id, update)
			}
		}
		//}
	} else {
		//for i, update := range updates {

		res, err := collectionHandle.UpdateOne(context.Background(), id, update, nil)
		if err != nil && mongo.IsDuplicateKeyError(err) == false {
			log.Warn("update _id[%v] with data[%v] meets err[%v] res[%v], try to solve", id, update, err, res)

			// error can be ignored
			if IgnoreError(err, "u",
				checkpoint.ToInt64(l.ParsedLog.Timestamp) <= sw.fullFinishTs) {
				return nil
			}

			log.Error("update _id[%v] with data[%v] failed[%v]", id, update, err.Error())
			return err
		}
		if res != nil {
			if res.MatchedCount != 1 {
				return fmt.Errorf("Update fail(MatchedCount:%d, ModifiedCount:%d) old-data[%v] with new-data[%v]",
					res.MatchedCount, res.ModifiedCount, id, update)
			}
		}
		//}
	}

	return nil
}

// Update the document
func (ow *OplogWriter) handleUpdate(l *ChangeLog, upsert bool) error {

	// Get the collection
	collectionHandle := mong.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	var err error
	var res *mongo.UpdateResult

	// Below we check if the object has a version mark which is identified by "$v"
	if FindFiledPrefix(l.Object, "$v") {

		// Get the version of the OPLog
		oplogVer, ok := GetKey(l.Object, VersionMark).(int32)

		// To keep track of the update
		var update interface{}

		// TODO Simplify this as we are running on modern version of MongoDB
		if ok && oplogVer == 2 {
			var oplogErr error
			if update, oplogErr = DiffUpdateOplogToNormal(l.Object); oplogErr != nil {
				log.ErrorWithFields("Update failed", log.Fields{
					"err":     oplogErr,
					"org_doc": l.Object,
				})
				return oplogErr
			}
		} else {
			log.Warn("Unknown version of OPLog: %v", l.Object)

			// l.Object = RemoveFiled(l.Object, VersionMark)
			// update = l.Object
		}

		updateOpts := options.Update()
		if upsert {
			updateOpts.SetUpsert(true)
		}
		if upsert && len(l.DocumentKey) > 0 {
			res, err = collectionHandle.UpdateOne(context.Background(), l.ParsedLog.DocumentKey, update, updateOpts)
		} else {
			// if upsert {
			// 	log.Warn("Upsert but lack documentKey: %v", l)
			// }

			// Query contains the object id triggering the update
			res, err = collectionHandle.UpdateOne(context.Background(), l.ParsedLog.Query, update, updateOpts)
		}
	} else {
		log.Warn("Normal update")
	}
	/*else {
		update = l.Object
		opts := options.Replace()
		if upsert {
			opts.SetUpsert(true)
		}
		if upsert && len(l.DocumentKey) > 0 {
			res, err = collectionHandle.ReplaceOne(context.Background(), l.ParsedLog.DocumentKey, update, opts)
		} else {
			res, err = collectionHandle.ReplaceOne(context.Background(), l.ParsedLog.Query, update, opts)
		}
	}*/

	if err != nil {
		// error can be ignored
		if IgnoreError(err, "u",
			checkpoint.ToInt64(l.ParsedLog.Timestamp) <= ow.fullFinishTs) {
			return nil
		}

		if mongo.IsDuplicateKeyError(err) {
			log.Error("Duplicate key error: %v", err)
			//RecordDuplicatedOplog(sw.conn, collection, oplogs)
			return nil
		}

		log.Error("doUpdate[upsert] old-data[%v] with new-data[%v] failed[%v]",
			l.ParsedLog.Query, l.ParsedLog.Object, err)
		return err
	}
	if res != nil {
		if res.MatchedCount != 1 {
			return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d MatchedCount:%d) old-data[%v] with new-data[%v]")

		}
	}
	if upsert {
		if res.MatchedCount != 1 && res.UpsertedCount != 1 {
			return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d) old-data[%v] with new-data[%v]",
				res.MatchedCount, res.ModifiedCount, res.UpsertedCount,
				l.ParsedLog.Query, l.ParsedLog.Object)
		}
	} else {
		if res.MatchedCount != 1 {
			return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d MatchedCount:%d) old-data[%v] with new-data[%v]",
				res.MatchedCount, res.ModifiedCount, res.MatchedCount,
				l.ParsedLog.Query, l.ParsedLog.Object)
		}
	}

	return nil
}

func (ow *OplogWriter) handleDelete(log *ChangeLog) error {
	// Delete the document
	return nil
}

// true means error can be ignored
// https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
func IgnoreError(err error, op string, isFullSyncStage bool) bool {
	if err == nil {
		return true
	}

	er, ok := err.(mongo.ServerError)
	if !ok {
		return false
	}

	switch op {
	case "i":
		/*if isFullSyncStage {
			if err == 11000 { // duplicate key
				continue
			}
		}*/
	case "u":
		if isFullSyncStage {
			if er.HasErrorCode(28) || er.HasErrorCode(211) { // PathNotViable
				return true
			}
		}
	case "ui":
		if isFullSyncStage {
			if er.HasErrorCode(11000) { // duplicate key
				return true
			}
		}
	case "d":
		if er.HasErrorCode(26) { // NamespaceNotFound
			return true
		}
	case "c":
		if er.HasErrorCode(26) { // NamespaceNotFound
			return true
		}
	default:
		return false
	}

	return false
}

func debugLog(l *ParsedLog) {
	// Try to get the object id
	id, _ := GetObjectId(l.Object)
	log.DebugWithFields("OPLOG entry: ", log.Fields{
		"ns": l.Namespace,
		"op": l.Operation,
		"ts": l.Timestamp,
		"id": id,
	})
}
