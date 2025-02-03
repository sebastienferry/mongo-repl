package snapshot

import (
	"context"
	"log"
	"testing"
)

func TestReadItems(t *testing.T) {

	db1 := NewMockDatabase(CreateTestData(1, 2, 3, 50, 100, 101, 105))
	db2 := NewMockDatabase(CreateTestData(1, 2, 3, 5, 6, 7, 8, 9, 200, 201))

	sync := NewDeltaReplication(db1, db2, db2, "test", "test", false, 2)
	sync.SynchronizeCollection(context.Background())

	for i, item := range db2.Items {
		oid := GetObjectId(item)
		log.Printf("Item %d: %s", i, oid.Hex())
	}
}
