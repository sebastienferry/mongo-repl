package filters

import (
	"strings"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
)

var AllowedOperation = map[string]bool{
	"applyOps":         true,
	"startIndexBuild":  true,
	"commitIndexBuild": true,
	"abortIndexBuild":  true,
	"dropIndex":        false,
	"dropIndexes":      true,
	// "create":           false,
	// "createIndexes":    false,
	// "collMod":          false,
	// "dropDatabase":     false,
	// "drop":             false,
	// "deleteIndex":      false,
	// "deleteIndexes":    false,
	// "renameCollection": false,
	// "convertToCapped":  false,
	// "emptycapped":      false,
}

var (
	AllowedOperations = map[string]bool{
		oplog.NoOp: false, // no-op
		oplog.DbOp: false, // database

		// Keep the following operations
		oplog.UpdateOp:  true, // update
		oplog.DeleteOp:  true, // delete
		oplog.InsertOp:  true, // insert
		oplog.CommandOp: true, // command
	}

	AllowedOperationsForApplyOps = map[string]bool{
		oplog.UpdateOp: true, // update
		oplog.DeleteOp: true, // delete
		oplog.InsertOp: true, // insert
	}
)

func Lookup(items map[string]bool, item string) (bool, bool) {
	value := false
	found := false

	if len(items) == 0 {
		return value, found
	}

	value, found = items[item]
	return value, found
}

func ShouldReplicateNamespace(
	databasesIn map[string]bool,
	collectionsIn map[string]bool,
	collectionsOut map[string]bool,
	db string, collection string) bool {

	// Check if the database is part of the one we are targeting
	if ok := databasesIn[db]; !ok {
		return false
	}

	if len(collectionsIn) > 0 {
		// If we include collections explicitly, check if the collection is in
		val, found := Lookup(collectionsIn, collection)
		return found && val
	} else if len(collectionsOut) > 0 {
		// If we exclude collections explicitly, check if the collection is not out
		val, found := Lookup(collectionsIn, collection)
		return !found || !val
	}
	return true
}

type Filter struct {
	allowedOperations map[string]bool
}

func NewFilter() *Filter {
	return &Filter{}
}

// Filter out unwanted operations
func (f *Filter) KeepOperation(operation string) bool {
	val, found := Lookup(AllowedOperations, operation)
	return found && val
}

// Filter out unwanted namespaces
func (f *Filter) KeepCollection(db string, collection string) bool {

	if collection == "" || db == "" {
		return false
	}

	// Filter unwanted data
	if !ShouldReplicateNamespace(
		config.Current.Repl.DatabasesIn,
		config.Current.Repl.FiltersIn,
		config.Current.Repl.FiltersOut,
		db, collection) {
		return false
	}
	return true

}

func KeepOperation(command string) bool {
	if keep, ok := AllowedOperation[strings.TrimSpace(command)]; ok {
		return keep
	}
	return false
}
