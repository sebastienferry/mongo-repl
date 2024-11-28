package filter

import (
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
)

type Filter struct {
	filteredOperations map[string]bool
}

func NewFilter() *Filter {
	return &Filter{
		filteredOperations: map[string]bool{
			"n":  true, // no-op
			"c":  true, // command
			"db": true, // database

			// Keep the following operations
			"u": false, // update
			"d": false, // delete
			"i": false, // insert
		},
	}
}

// Filter out unwanted operations
func (f *Filter) KeepOperation(operation string) bool {
	return !f.filteredOperations[operation]
}

// Filter out unwanted namespaces
func (f *Filter) KeepCollection(db string, collection string) bool {

	if collection == "" || db == "" {
		return false
	}

	// Filter unwanted data
	if !shouldReplicate(db, collection) {
		return false
	}
	return true

}

func shouldReplicate(db string, collection string) bool {

	// Check if the database is part of the one we are targeting
	if ok := config.Current.Repl.DatabasesIn[db]; !ok {
		return false
	}

	if len(config.Current.Repl.FiltersIn) > 0 {
		if _, ok := config.Current.Repl.FiltersIn[collection]; !ok {
			return false
		}
		return true
	} else if config.Current.Repl.FiltersOut[collection] {
		return false
	}
	return true
}
