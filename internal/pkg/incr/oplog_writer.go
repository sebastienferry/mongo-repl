package incr

import (
	"context"
	"fmt"
	"strings"

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

			switch l.Operation {
			case "i":
				handleInsert(l)
			case "u":
				ow.handleUpdate(l, true)
			case "d":
				handleDelete(l)
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

func handleInsert(log *ChangeLog) {
	// Insert the document

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

func handleDelete(log *ChangeLog) {
	// Delete the document
}

func FindFiledPrefix(input bson.D, prefix string) bool {
	for id := range input {
		if strings.HasPrefix(input[id].Key, prefix) {
			return true
		}
	}

	return false
}

// pay attention: the input bson.D will be modified.
func RemoveFiled(input bson.D, key string) bson.D {
	flag := -1
	for id := range input {
		if input[id].Key == key {
			flag = id
			break
		}
	}

	if flag != -1 {
		input = append(input[:flag], input[flag+1:]...)
	}
	return input
}

// "o" : { "$v" : 2, "diff" : { "d" : { "count" : false }, "u" : { "name" : "orange" }, "i" : { "c" : 11 } } }
func DiffUpdateOplogToNormal(updateObj bson.D) (interface{}, error) {

	diffObj := GetKey(updateObj, "diff")
	if diffObj == nil {
		return updateObj, fmt.Errorf("don't have diff field updateObj:[%v]", updateObj)
	}

	bsonDiffObj, ok := diffObj.(bson.D)
	if !ok {
		return updateObj, fmt.Errorf("diff field is not bson.D updateObj:[%v]", updateObj)
	}

	result, err := BuildUpdateDelteOplog("", bsonDiffObj)
	if err != nil {
		return updateObj, fmt.Errorf("parse diffOplog failed updateObj:[%v] err[%v]", updateObj, err)
	}

	return result, nil

}

func GetKey(log bson.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(log, wanted)
	return ret
}

func GetKeyWithIndex(log bson.D, wanted string) (interface{}, int) {
	if wanted == "" {
		wanted = "_id"
	}

	// "_id" is always the first field
	for id, ele := range log {
		if ele.Key == wanted {
			return ele.Value, id
		}
	}

	return nil, 0
}

func BuildUpdateDelteOplog(prefixField string, obj bson.D) (interface{}, error) {
	var result bson.D

	for _, ele := range obj {
		if ele.Key == "d" {
			result = append(result, primitive.E{
				Key:   "$unset",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if ele.Key == "i" || ele.Key == "u" {
			result = append(result, primitive.E{
				Key:   "$set",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if len(ele.Key) > 1 && ele.Key[0] == 's' {
			// s means subgroup field(array or nest)
			tmpPrefixField := ""
			if len(prefixField) == 0 {
				tmpPrefixField = ele.Key[1:]
			} else {
				tmpPrefixField = prefixField + "." + ele.Key[1:]
			}

			nestObj, err := BuildUpdateDelteOplog(tmpPrefixField, ele.Value.(bson.D))
			if err != nil {
				return obj, fmt.Errorf("parse ele[%v] failed, updateObj:[%v]", ele, obj)
			}
			if _, ok := nestObj.(mongo.Pipeline); ok {
				return nestObj, nil
			} else if _, ok := nestObj.(bson.D); ok {
				for _, nestObjEle := range nestObj.(bson.D) {
					result = append(result, nestObjEle)
				}
			} else {
				return obj, fmt.Errorf("unknown nest type ele[%v] updateObj:[%v] nestObj[%v]", ele, obj, nestObj)
			}

		} else if len(ele.Key) > 1 && ele.Key[0] == 'u' {
			result = append(result, primitive.E{
				Key: "$set",
				Value: bson.D{
					primitive.E{
						Key:   prefixField + "." + ele.Key[1:],
						Value: ele.Value,
					},
				},
			})

		} else if ele.Key == "l" {
			if len(result) != 0 {
				return obj, fmt.Errorf("len should be 0, Key[%v] updateObj:[%v], result:[%v]",
					ele, obj, result)
			}

			return mongo.Pipeline{
				{{"$set", bson.D{
					{prefixField, bson.D{
						{"$slice", []interface{}{"$" + prefixField, ele.Value}},
					}},
				}}},
			}, nil

		} else if ele.Key == "a" && ele.Value == true {
			continue
		} else {
			return obj, fmt.Errorf("unknow Key[%v] updateObj:[%v]", ele, obj)
		}
	}

	return result, nil
}

func combinePrefixField(prefixField string, obj interface{}) interface{} {
	if len(prefixField) == 0 {
		return obj
	}

	tmpObj, ok := obj.(bson.D)
	if !ok {
		return obj
	}

	var result bson.D
	for _, ele := range tmpObj {
		result = append(result, primitive.E{
			Key:   prefixField + "." + ele.Key,
			Value: ele.Value})
	}

	return result
}

// func (ow *OplogWriter) getWriteOperation(log *ParsedLog) mongo.WriteModel {

// 	switch log.Operation {
// 	case "i":
// 		if len(log.DocumentKey) > 0 {
// 			// update
// 			return mongo.NewUpdateOneModel().SetFilter(log.DocumentKey).SetUpdate(log.Object)
// 		} else {
// 			// insert
// 			return mongo.NewInsertOneModel().SetDocument(log.Object)
// 		}
// 	case "u":
// 	case "d":
// 	}
// 	return nil
// }

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

func GetObjectId(log bson.D) (primitive.ObjectID, error) {
	for _, bsonE := range log {
		if bsonE.Key == "_id" {
			if oid, ok := bsonE.Value.(primitive.ObjectID); ok {
				return oid, nil
			}
		}
	}
	return primitive.ObjectID{}, fmt.Errorf("No ObjectID found")
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

//opts := options.BulkWrite().SetOrdered(false)
//res, err := mong.Registry.GetTarget().Client.Database("DeliveryCache").Collection(collection).BulkWrite(nil, models, opts)
