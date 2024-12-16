package incr

import (
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/filter"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	Uuid     = "ui"
	ApplyOps = "applyOps"
)

// Filter an applyOps command sub-operations
func FilterApplyOps(ele primitive.E, keepSubOp func(primitive.D) bool, computedCmd primitive.D, computedCmdSize int) (primitive.D, int) {
	var j = 0
	switch subOps := ele.Value.(type) {
	case bson.A:
		for _, subOp := range subOps {
			doc := subOp.(bson.D)
			if !keepSubOp(doc) {
				continue
			}
			subOps[j] = doc
			j++
		}
		if j > 0 {
			computedCmd = append(computedCmd, primitive.E{Key: ele.Key, Value: subOps[:j]})
			computedCmdSize += j
		}
	}
	return computedCmd, computedCmdSize
}

// Check if a sub-operation from an applyOps command should be kept
// We only keep insert, update and delete operations
// We also apply filter on the namespace
func KeepSubOp(doc bson.D) bool {

	// Filter the op
	op := GetKey(doc, "op").(string)
	allowed, found := filter.Lookup(filter.AllowedOperationsForApplyOps, op)
	if !found || !allowed {
		return false
	}

	// Filter the namespace
	ns := GetKey(doc, "ns")
	subDb, subColl := oplog.GetDbAndCollection(ns.(string))

	return filter.ShouldReplicateNamespace(
		config.Current.Repl.DatabasesIn,
		config.Current.Repl.FiltersIn,
		config.Current.Repl.FiltersOut,
		subDb, subColl)
}

func RunCommand(database, command string, l *oplog.ChangeLog, client *mongo.Client) error {

	log.DebugWithFields("Execute DDL command", log.Fields{"command": command})

	switch command {
	case "createIndexes":
		// createIndexes command
		log.Debug("createIndexes command")
	case "commitIndexBuild":
		// commitIndexBuild command
		log.Debug("commitIndexBuild command")
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applysOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		var store bson.D
		for _, ele := range l.Object {
			if ApplyOpsFilter(ele.Key) {
				continue
			}
			if ele.Key == "applyOps" {
				switch v := ele.Value.(type) {
				case bson.A:
					for i, ele := range v {
						doc := ele.(bson.D)

						//TODO: Filter out the unwated collection.

						v[i] = RemoveField(doc, Uuid)
					}
				case []interface{}:
					for i, ele := range v {
						doc := ele.(bson.D)
						v[i] = RemoveField(doc, Uuid)
					}
				case bson.D:
					ret := make(bson.D, 0, len(v))
					for _, ele := range v {
						if ele.Key == Uuid {
							continue
						}
						ret = append(ret, ele)
					}
					ele.Value = ret
				case []bson.M:
					for _, ele := range v {
						delete(ele, Uuid)
					}
				}

			}
			store = append(store, ele)
		}
		singleResult := mdb.Registry.GetTarget().Client.Database(database).RunCommand(nil, store)
		raw, _ := singleResult.Raw()

		var content bson.M
		if raw != nil {
			content = bson.M{}
			bson.Unmarshal(raw, &content)
		}

		if singleResult.Err() != nil {
			log.ErrorWithFields("Error running applyOps command", log.Fields{"error": singleResult.Err()})
		} else {
			log.DebugWithFields("command executed", log.Fields{"command": store, "applied": content["applied"].(int32)})
		}
	case "dropDatabase":
		// dropDatabase command
		log.Debug("dropDatabase command")
	case "create":
		// create command
		log.Debug("create command")
	case "collMod":
		// collMod command
		log.Debug("collMod command")
	case "drop":
		// drop command
		log.Debug("drop command")
	case "deleteIndex":
		// deleteIndex command
		log.Debug("deleteIndex command")
	case "deleteIndexes":
		// deleteIndexes command
		log.Debug("deleteIndexes command")
	case "dropIndex":
		// dropIndex command
		log.Debug("dropIndex command")
	case "dropIndexes":
		// dropIndexes command
		log.Debug("dropIndexes command")
	case "convertToCapped":
		// convertToCapped command
		log.Debug("convertToCapped command")
	case "renameCollection":
		// renameCollection command
		log.Debug("renameCollection command")
	case "emptycapped":
		// emptycapped command
		log.Debug("emptycapped command")
	default:
		// default command
		log.Debug("default command")
	}

	return nil

	// defer LOG.Debug("RunCommand run DDL: %v", log.Dump(nil, true))
	// dbHandler := client.Database(database)
	// LOG.Info("RunCommand run DDL with type[%s]", operation)
	// var err error
	// switch command {
	// case "createIndexes":
	// 	/*
	// 	* after v3.6, the given oplog should have uuid when run applyOps with createIndexes.
	// 	* so we modify oplog base this ref:
	// 	* https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes
	// 	 */
	// 	var innerBsonD, indexes bson.D
	// 	for i, ele := range log.Object {
	// 		if i == 0 {
	// 			nimo.AssertTrue(ele.Key == "createIndexes", "should panic when ele.Name != 'createIndexes'")
	// 		} else {
	// 			innerBsonD = append(innerBsonD, ele)
	// 		}
	// 	}
	// 	indexes = append(indexes, log.Object[0]) // createIndexes
	// 	indexes = append(indexes, primitive.E{
	// 		Key: "indexes",
	// 		Value: []bson.D{ // only has 1 bson.D
	// 			innerBsonD,
	// 		},
	// 	})
	// 	err = dbHandler.RunCommand(nil, indexes).Err()
	// case "commitIndexBuild":
	// 	/*
	// 		If multiple indexes are created, commitIndexBuild only generate one oplog, CreateIndexes multiple oplogs
	// 		{ "op" : "c", "ns" : "test.$cmd", "ui" : UUID("617ffe90-6dac-4e71-b570-1825422c1896"),
	// 		  "o" : { "commitIndexBuild" : "car", "indexBuildUUID" : UUID("4e9b7457-b612-42bb-bbad-bd6e9a2d63a7"),
	// 		          "indexes" : [
	// 		                      { "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" },
	// 		                      { "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" }
	// 		                      ]},
	// 		  "ts" : Timestamp(1653620229, 6), "t" : NumberLong(1), "v" : NumberLong(2), "wall" : ISODate("2022-05-27T02:57:09.187Z") }

	// 		CreateIndexes Command: db.car.createIndexes([{"count":1},{"type":1}])
	// 		{ "ts" : Timestamp(1653620582, 3), "t" : NumberLong(2), "h" : NumberLong(0), "v" : 2, "op" : "c", "ns" : "test.$cmd", "ui" : UUID("51d35827-e8b5-4891-8818-41326718505d"), "wall" : ISODate("2022-05-27T03:03:02.282Z"), "o" : { "createIndexes" : "car", "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" } }
	// 		{ "ts" : Timestamp(1653620582, 2), "t" : NumberLong(2), "h" : NumberLong(0), "v" : 2, "op" : "c", "ns" : "test.$cmd", "ui" : UUID("51d35827-e8b5-4891-8818-41326718505d"), "wall" : ISODate("2022-05-27T03:03:02.281Z"), "o" : { "createIndexes" : "car", "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" } }
	// 	*/
	// 	var indexes bson.D
	// 	for i, ele := range log.Object {
	// 		if i == 0 {
	// 			indexes = append(indexes, primitive.E{
	// 				Key:   "createIndexes",
	// 				Value: ele.Value.(string),
	// 			})
	// 			nimo.AssertTrue(ele.Key == "commitIndexBuild", "should panic when ele.Name != 'commitIndexBuild'")
	// 		} else {
	// 			if ele.Key == "indexes" {
	// 				indexes = append(indexes, primitive.E{
	// 					Key:   "indexes",
	// 					Value: ele.Value,
	// 				})
	// 			}
	// 		}
	// 	}

	// 	nimo.AssertTrue(len(indexes) >= 2, "indexes must at least have two elements")
	// 	LOG.Debug("RunCommand commitIndexBuild oplog after conversion[%v]", indexes)
	// 	err = dbHandler.RunCommand(nil, indexes).Err()
	// case "applyOps":
	// 	/*
	// 	 * Strictly speaking, we should handle applysOps nested case, but it is
	// 	 * complicate to fulfill, so we just use "applyOps" to run the command directly.
	// 	 */
	// 	var store bson.D
	// 	for _, ele := range log.Object {
	// 		if utils.ApplyOpsFilter(ele.Key) {
	// 			continue
	// 		}
	// 		if ele.Key == "applyOps" {
	// 			switch v := ele.Value.(type) {
	// 			case []interface{}:
	// 				for i, ele := range v {
	// 					doc := ele.(bson.D)
	// 					v[i] = oplog.RemoveFiled(doc, uuidMark)
	// 				}
	// 			case bson.D:
	// 				ret := make(bson.D, 0, len(v))
	// 				for _, ele := range v {
	// 					if ele.Key == uuidMark {
	// 						continue
	// 					}
	// 					ret = append(ret, ele)
	// 				}
	// 				ele.Value = ret
	// 			case []bson.M:
	// 				for _, ele := range v {
	// 					if _, ok := ele[uuidMark]; ok {
	// 						delete(ele, uuidMark)
	// 					}
	// 				}
	// 			}

	// 		}
	// 		store = append(store, ele)
	// 	}
	// 	err = dbHandler.RunCommand(nil, store).Err()
	// case "dropDatabase":
	// 	err = dbHandler.Drop(nil)
	// case "create":
	// 	if oplog.GetKey(log.Object, "autoIndexId") != nil &&
	// 		oplog.GetKey(log.Object, "idIndex") != nil {
	// 		// exits "autoIndexId" and "idIndex", remove "autoIndexId"
	// 		log.Object = oplog.RemoveFiled(log.Object, "autoIndexId")
	// 	}
	// 	fallthrough
	// case "collMod":
	// 	fallthrough
	// case "drop":
	// 	fallthrough
	// case "deleteIndex":
	// 	fallthrough
	// case "deleteIndexes":
	// 	fallthrough
	// case "dropIndex":
	// 	fallthrough
	// case "dropIndexes":
	// 	fallthrough
	// case "convertToCapped":
	// 	fallthrough
	// case "renameCollection":
	// 	fallthrough
	// case "emptycapped":
	// 	if !oplog.IsRunOnAdminCommand(command) {
	// 		err = dbHandler.RunCommand(nil, log.Object).Err()
	// 	} else {
	// 		err = client.Database("admin").RunCommand(nil, log.Object).Err()
	// 	}
	// default:
	// 	LOG.Info("type[%s] not found, use applyOps", command)

	// 	// filter log.Object
	// 	var rec bson.D
	// 	for _, ele := range log.Object {
	// 		if utils.ApplyOpsFilter(ele.Key) {
	// 			continue
	// 		}

	// 		rec = append(rec, ele)
	// 	}
	// 	log.Object = rec // reset log.Object

	// 	var store bson.D
	// 	store = append(store, primitive.E{
	// 		Key: "applyOps",
	// 		Value: []bson.D{
	// 			log.Dump(nil, true),
	// 		},
	// 	})
	// 	err = dbHandler.RunCommand(nil, store).Err()
	// }

	// return err
}
