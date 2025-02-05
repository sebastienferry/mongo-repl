package snapshot

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/sebastienferry/mongo-repl/internal/pkg/mocks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Convert the bytes as 12 bytes arrays
func CreateTestData(items ...byte) []*bson.D {
	var data []*bson.D
	for _, i := range items {
		data = append(data, &bson.D{{Key: "_id",
			Value: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i})}})
	}
	return data
}

func TestCompareAndSync(t *testing.T) {

	var data = []struct {
		name     string
		source   []*bson.D
		target   []*bson.D
		expected int
	}{
		{
			// Both source and target are nil
			"null everything",
			nil, nil, 0,
		},
		{
			// Both source and target are empty
			"empty everything",
			[]*bson.D{}, []*bson.D{}, 0,
		},
		{
			// Only source has data: Add the all
			"add all",
			CreateTestData(1, 2, 3, 4), nil, 4,
		},
		{
			// Only target has data : Drop them all
			"drop all",
			nil, CreateTestData(1, 2, 3, 4), 0,
		},
		{
			// Source and target are the same : Update them all
			"update all",
			CreateTestData(1, 2, 3, 4), CreateTestData(1, 2, 3, 4), 4,
		},
		{
			// Source has less data than target : Remove the extra
			"remove extra",
			CreateTestData(1, 3), CreateTestData(1, 2, 3, 4), 2,
		},
		{
			// Source has more data than target : Add the missing
			// 3 is updated, 8, 9, 12 are added and 4, 5 are removed
			"add missing",
			CreateTestData(1, 3, 8, 9, 12), CreateTestData(2, 3, 4, 5), 5,
		},
		{
			"more advanced case",
			CreateTestData(1, 2, 3, 50, 100, 101, 105),
			CreateTestData(1, 2, 3, 5, 6, 7, 8, 9, 200, 201),
			7,
		},
	}

	// Testing various batch sizes
	// 1, 2, 4, 8, 16, 32, 64
	for batchSize := 1; batchSize < 100; batchSize = batchSize * 2 {

		for _, d := range data {

			log.Printf("case: %s, Batch size %d\n", d.name, batchSize)
			source := mocks.NewMockDatabase(d.source)
			target := mocks.NewMockDatabase(d.target)
			//writer := NewMockDatabase(d.target)

			synchronization := NewDeltaReplication(source, target, target, "test", "test", false, batchSize)
			synchronization.SynchronizeCollection(context.TODO())

			// Check if the data was inserted
			if len(target.Items) != d.expected {
				t.Errorf(fmt.Sprintf("case %s, Expected %d item, got", d.name, d.expected), len(target.Items))
			}
		}

	}
}
