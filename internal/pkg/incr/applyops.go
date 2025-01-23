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

func RunCommandApplyOps(database string, l *oplog.ChangeLog, client *mongo.Client) error {
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
	return singleResult.Err()
}
