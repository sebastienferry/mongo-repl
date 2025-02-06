package filters

import (
	"testing"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
)

func TestLookup(t *testing.T) {
	operations := map[string]bool{
		"op1": true,
		"op2": false,
	}

	tests := []struct {
		operation string
		expected  bool
	}{
		{"", false},
		{"op1", true},
		{"op2", false},
		{"op3", false},
	}

	for _, test := range tests {
		allowed, found := Lookup(operations, test.operation)
		if found && (allowed != test.expected) {
			t.Errorf("KeepOperation(%s) = %v; want %v", test.operation, allowed, test.expected)
		}
	}
}

func TestShouldReplicateNamespace(t *testing.T) {
	databasesIn :=
		map[string]bool{
			"db1": true,
			"db2": true,
		}
	collectionsIn :=
		map[string]bool{
			"coll1": true,
			"coll2": true,
		}
	collectionsOut :=
		map[string]bool{
			"coll3": true,
		}

	tests := []struct {
		db         string
		collection string
		expected   bool
	}{
		// Explicitly allowed
		{"db1", "coll1", true},
		{"db1", "coll2", true},
		{"db2", "coll1", true},
		{"db2", "coll2", true},
		// Explicitly denied
		{"db1", "coll3", false},
		{"db2", "coll3", false},
		// Unspecified with database allowed, should be true as a wild card
		{"db1", "coll4", true},
		{"db2", "coll4", true},
		// Database denied, all collections should be denied
		{"db3", "coll1", false},
		{"db3", "coll2", false},
		{"db3", "coll3", false},
		{"db3", "coll4", false},
		// Unspecifies database, should be false
		{"db4", "coll1", false},
		{"db4", "coll3", false},
	}

	for _, test := range tests {
		result := ShouldReplicateNamespace(databasesIn, collectionsIn, collectionsOut, test.db, test.collection)
		if result != test.expected {
			t.Errorf("shouldReplicate(%s, %s) = %v; want %v", test.db, test.collection, result, test.expected)
		}
	}
}

// Test both KeepOperation and KeepCollection
func TestOplogFilterKeepOperation(t *testing.T) {
	config.Current = &config.AppConfig{
		Repl: config.ReplConfig{
			DatabasesIn: map[string]bool{
				"db1": true,
			},
			FiltersIn: map[string]bool{
				"coll1": true,
			},
			FiltersOut: map[string]bool{},
		},
	}

	filter := NewFilter()

	tests := []struct {
		db         string
		collection string
		operation  string
		expected   bool
	}{
		{"", "", "", false},
		{"", "", "i", false},
		{"", "coll", "i", false},
		{"db", "", "i", false},

		{"db1", "coll1", "i", true},
		{"db1", "coll1", "u", true},
		{"db1", "coll1", "d", true},

		{"db1", "coll2", "i", true},
		{"db1", "coll2", "u", true},
		{"db1", "coll2", "d", true},

		{"db2", "coll1", "i", false},
		{"db2", "coll1", "u", false},
		{"db2", "coll1", "d", false},
	}

	for _, test := range tests {
		result := filter.KeepOperation(test.operation) && filter.KeepCollection(test.db, test.collection)
		if result != test.expected {
			t.Errorf("Keep(%s, %s, %s) = %v; want %v", test.db, test.collection, test.operation, result, test.expected)
		}
	}
}
