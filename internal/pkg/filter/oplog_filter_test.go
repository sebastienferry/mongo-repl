package filter

import (
	"testing"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
)

func TestOplogFilter_Keep(t *testing.T) {
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

	filter := NewOplogFilter()

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

		{"db1", "coll2", "i", false},
		{"db1", "coll2", "u", false},
		{"db1", "coll2", "d", false},

		{"db2", "coll1", "i", false},
		{"db2", "coll1", "u", false},
		{"db2", "coll1", "d", false},
	}

	for _, test := range tests {
		result := filter.Keep(test.db, test.collection, test.operation)
		if result != test.expected {
			t.Errorf("Keep(%s, %s, %s) = %v; want %v", test.db, test.collection, test.operation, result, test.expected)
		}
	}
}
