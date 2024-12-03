// -----------------------------------------------------------------------------
// oplog_writer_single.go
// -----------------------------------------------------------------------------
// This file contains the implementation of the OplogWriter struct which is
// responsible for writing the oplog entries to the target database. The struct
// contains a channel that receives ChangeLog entries and processes them. The
// StartWriter method is used to start the writer goroutine that processes the
// ChangeLog entries. The handleInsert, handleUpdate, and handleDelete methods
// are used to process the respective operations.

package incr

import (
	"context"
	"fmt"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	VersionMark       = "$v"
	OplogVersionError = "unknown version of OPLog"
	OperationError    = "operation error"
	InsertError       = "insert error"
	UpdateError       = "update error"
	DeleteError       = "delete error"
	UpsertError       = "upsert error"
	DuplicateError    = "duplicate error"
	MatchedCountError = "matched count error"
)

type OplogWriterSingle struct {
	queuedLogs   chan *oplog.ChangeLog
	fullFinishTs int64
	done         chan bool
	ckptManager  checkpoint.CheckpointManager
}

func NewOplogWriter(fullFinishTs int64, queuedLogs chan *oplog.ChangeLog, ckptManager checkpoint.CheckpointManager) *OplogWriterSingle {
	return &OplogWriterSingle{
		queuedLogs:   queuedLogs,
		fullFinishTs: fullFinishTs,
		done:         make(chan bool),
		ckptManager:  ckptManager,
	}
}

func (ow *OplogWriterSingle) StopWriter() {
	ow.done <- true
}

// Start the writer
func (ow *OplogWriterSingle) StartWriter(ctx context.Context) {

	log.Info("starting oplog writer")

	go func() {
		for l := range ow.queuedLogs {

			// Check if we should stop processing
			select {
			case <-ow.done:
				log.Info("Stopping oplog writer")
				return
			default:
			}

			if l.Version != 2 {
				log.Warn(OplogVersionError, log.Fields{"version": l.Version})
				continue
			}

			// Try to get the object id
			var id interface{}
			if l.Operation != "c" {
				pid, _ := GetObjectId(l.Object)
				if pid.IsZero() && len(l.Query) > 0 {
					id = GetKey(l.Query, "_id")
				} else {
					id = pid
				}
				// Display some debug information
				debugLog(id, &l.ParsedLog)
			}

			// Handle the operation
			var opErr error = nil
			switch l.Operation {
			case "i":
				opErr = ow.Insert(l)
			case "u":
				opErr = ow.Update(l, true)
			case "d":
				opErr = ow.Delete(l)
			case "c":
				opErr = ow.Command(l)
			}

			// Check for errors
			if opErr != nil {
				log.ErrorWithFields(OperationError, log.Fields{
					"err": opErr,
					"op":  l.Operation,
					"id":  id,
				})
			}

			metrics.IncrSyncOplogWriteCounter.WithLabelValues(l.Db, l.Collection, l.Operation).Inc()
			metrics.CheckpointGauge.Set(float64(l.ParsedLog.Timestamp.T))

			// Save the checkpoint
			ow.ckptManager.MoveCheckpointForward(l.Timestamp)
		}

		// We should not reach this point
		log.Error("oplog writer stopped unexpectedly")

	}()
}

func (ow *OplogWriterSingle) Insert(l *oplog.ChangeLog) error {

	// DB Connection
	collectionHandle := mdb.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	// Insert the document
	if _, err := collectionHandle.InsertOne(context.Background(), l.ParsedLog.Object); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			err = ow.Upsert(l, true)
			return err
		} else {
			log.ErrorWithFields(InsertError, log.Fields{"err": err})
			return err
		}
	}
	return nil

}

// Upsert the document
func (sw *OplogWriterSingle) Upsert(l *oplog.ChangeLog, upsert bool) error {

	// DB Connection
	collectionHandle := mdb.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	var id interface{}
	var update interface{} = bson.D{{"$set", l.ParsedLog.Object}}

	if upsert && len(l.ParsedLog.DocumentKey) > 0 {
		id = l.ParsedLog.DocumentKey
	} else {
		id = GetKey(l.ParsedLog.Object, "")
		if id != nil {
			id = bson.D{{"_id", id}}
		} else {
			return fmt.Errorf("insert on duplicated update _id look up failed. %v", l.ParsedLog)
		}
	}

	// Upsert activated or not?
	var updateOpts *options.UpdateOptions = options.Update()
	if upsert {
		updateOpts = updateOpts.SetUpsert(true)
	}

	// Update the document
	res, err := collectionHandle.UpdateOne(context.Background(), id, update, updateOpts)
	if err != nil {

		log.WarnWithFields(UpsertError, log.Fields{
			"id":     id,
			"update": update,
			"err":    err,
			"res":    res,
		})

		// error can be ignored(insert fail & oplog is before full end)
		if mongo.IsDuplicateKeyError(err) && checkpoint.ToInt64(l.ParsedLog.Timestamp) <= sw.fullFinishTs {
			return nil
		}

		log.ErrorWithFields(UpsertError, log.Fields{
			"id":     id,
			"update": update,
			"err":    err,
		})

		return err
	}

	if res != nil {
		if res.MatchedCount != 1 && res.UpsertedCount != 1 {
			return fmt.Errorf("update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d) upsert _id[%v] with data[%v]",
				res.MatchedCount, res.ModifiedCount, res.UpsertedCount, id, update)
		}
	}

	return nil
}

// Update the document
func (ow *OplogWriterSingle) Update(l *oplog.ChangeLog, upsert bool) error {

	// DB Connection
	collectionHandle := mdb.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)

	var err error
	var res *mongo.UpdateResult

	// Below we check if the object has a version mark which is identified by "$v"
	if FindFiledPrefix(l.Object, "$v") {

		// To keep track of the update
		var update interface{}
		var oplogErr error
		if update, oplogErr = DiffUpdateOplogToNormal(l.Object); oplogErr != nil {
			log.ErrorWithFields("Update failed", log.Fields{
				"err":     oplogErr,
				"org_doc": l.Object,
			})
			return oplogErr
		}

		updateOpts := options.Update()
		if upsert {
			updateOpts.SetUpsert(true)
		}

		if upsert && len(l.DocumentKey) > 0 {
			res, err = collectionHandle.UpdateOne(context.Background(), l.ParsedLog.DocumentKey, update, updateOpts)
		} else {
			res, err = collectionHandle.UpdateOne(context.Background(), l.ParsedLog.Query, update, updateOpts)
		}

		if err != nil {
			if IgnoreError(err, "u",
				checkpoint.ToInt64(l.ParsedLog.Timestamp) <= ow.fullFinishTs) {
				return nil
			}
			if mongo.IsDuplicateKeyError(err) {
				log.Error(DuplicateError, log.Fields{"err": err})
				//RecordDuplicatedOplog(sw.conn, collection, oplogs)
				return nil
			}
			log.Error(UpdateError, log.Fields{"err": err})
			return err
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

	} else {
		return fmt.Errorf("Update failed, no version mark found in the object")
	}

	return nil
}

func (ow *OplogWriterSingle) Delete(l *oplog.ChangeLog) error {
	collectionHandle := mdb.Registry.GetTarget().Client.Database(l.Db).Collection(l.Collection)
	_, err := collectionHandle.DeleteOne(context.Background(), l.ParsedLog.Object)
	if err != nil {
		log.ErrorWithFields(DeleteError, log.Fields{"err": err})
		return err
	}
	return nil
}

func (ow *OplogWriterSingle) Command(l *oplog.ChangeLog) error {

	// Extract the sub-command
	if command, found := ExtraCommandName(l.ParsedLog.Object); found && IsSyncDataCommand(command) {

		var err error
		if err = RunCommand(l.Db, command, l, mdb.Registry.GetTarget().Client); err == nil {
			log.InfoWithFields("execute cmd operation", log.Fields{"op": "c", "command": command})
		} else if err.Error() == "ns not found" {
			log.InfoWithFields("execute cmd operation, ignore error", log.Fields{"op": "c", "command": command})
		} else if IgnoreError(err, "c", checkpoint.ToInt64(l.Timestamp) <= ow.fullFinishTs) {
			return nil
		} else {
			return err
		}

	} else {
		log.WarnWithFields("execute cmd operation, command not found", log.Fields{"op": "c", "command": command})
	}
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

func debugLog(id interface{}, l *oplog.ParsedLog) {
	log.DebugWithFields("OPLOG", log.Fields{
		"ns": l.Namespace,
		"op": l.Operation,
		"ts": l.Timestamp,
		"id": id,
	})
}
