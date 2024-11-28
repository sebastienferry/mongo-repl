package oplog

import (
	"testing"
)

func TestGetDbAndCollection(t *testing.T) {
	tests := []struct {
		namespace          string
		expectedDb         string
		expectedCollection string
	}{
		{"", "", ""},
		{"database", "database", ""},
		{"database.collection", "database", "collection"},
		{"database.collection.subcollection", "database", "collection.subcollection"},
	}

	for _, test := range tests {
		db, collection := GetDbAndCollection(test.namespace)
		if db != test.expectedDb || collection != test.expectedCollection {
			t.Errorf("GetDbAndCollection(%s) = (%s, %s); want (%s, %s)", test.namespace, db, collection, test.expectedDb, test.expectedCollection)
		}
	}
}
