package snapshot

import (
	"bytes"
	"context"
	"log"
	"math"
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MockDatabse struct {
	Items []*bson.D
}

func NewMockDatabase(items []*bson.D) *MockDatabse {
	return &MockDatabse{
		Items: items,
	}
}

func (s *MockDatabse) Insert(ctx context.Context, item *primitive.D) error {

	s.Items = append(s.Items, item)
	slices.SortFunc(s.Items, func(i, j *bson.D) int {
		ii, _ := getObjectId(i)
		jj, _ := getObjectId(j)
		return bytes.Compare(ii[:], jj[:])
	})
	return nil
}

func (s *MockDatabse) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {
	return nil
}

func (s *MockDatabse) Delete(ctx context.Context, id primitive.ObjectID) error {

	index := slices.IndexFunc(s.Items, func(i *bson.D) bool {
		val, ok := getObjectId(i)
		if !ok {
			panic("Item does not have an ID")
		}
		return bytes.Compare(val[:], id[:]) == 0
	})

	if index >= 0 {
		s.Items = append(s.Items[:index], s.Items[index+1:]...)
	}
	return nil
}

func TestCompareAndSync(t *testing.T) {

	// var data = []struct {
	// 	source   ItemsResult
	// 	target   ItemsResult
	// 	expected int
	// }{
	// 	{
	// 		ItemsResult{
	// 			Items:     nil,
	// 			HighestId: primitive.ObjectID{},
	// 			Count:     0,
	// 		},
	// 		ItemsResult{
	// 			Items:     nil,
	// 			HighestId: primitive.ObjectID{},
	// 			Count:     0,
	// 		},
	// 		0,
	// 	},
	// 	{
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
	// 			Count:     4,
	// 		},
	// 		ItemsResult{
	// 			Items:     nil,
	// 			HighestId: primitive.ObjectID{},
	// 			Count:     0,
	// 		},
	// 		4,
	// 	},
	// 	{
	// 		ItemsResult{
	// 			Items:     nil,
	// 			HighestId: primitive.ObjectID{},
	// 			Count:     0,
	// 		},
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
	// 			Count:     4,
	// 		},
	// 		0,
	// 	},
	// 	{
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
	// 			Count:     4,
	// 		},
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
	// 			Count:     4,
	// 		},
	// 		4,
	// 	},
	// 	{
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}),
	// 			Count:     2,
	// 		},
	// 		ItemsResult{
	// 			Items: []*bson.D{
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
	// 				{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	// 			},
	// 			HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
	// 			Count:     4,
	// 		},
	// 		2,
	// 	},
	// }

	// synchronizer := NewMockSynchronizer()

	// for _, d := range data {
	// 	compareAndSync(context.TODO(), d.source, d.target, synchronizer)

	// 	// Check if the data was inserted
	// 	if len(synchronizer.Items) != d.expected {
	// 		t.Errorf(fmt.Sprintf("Expected %d item, got", d.expected), len(synchronizer.Items))
	// 	}
	// }
}

type MockItemReader struct {
	Items *[]*bson.D
	Index int
}

func NewMockItemReader(items *[]*bson.D) *MockItemReader {
	return &MockItemReader{
		Items: items,
		Index: 0,
	}
}

func (r *MockDatabse) ReadItems(ctx context.Context, batchSize int, startId primitive.ObjectID) ([]*bson.D, error) {

	var startIndex = 0

	n, found := slices.BinarySearchFunc(r.Items, startId, func(i *bson.D, target primitive.ObjectID) int {
		val, ok := getObjectId(i)
		if !ok {
			panic("Item does not have an ID")
		}
		return bytes.Compare(val[:], target[:])
	})

	if !found {
		startIndex = n
	} else {
		startIndex = n + 1
	}

	// Make sure we do not go out of bounds
	if startIndex+batchSize > len(r.Items) {
		batchSize = int(math.Max(0, float64(len(r.Items)-startIndex)))
	}

	return r.Items[startIndex : startIndex+batchSize], nil
}

func TestSynchronizeCollection(t *testing.T) {

	source := NewMockDatabase([]*bson.D{
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
	})

	targetData := []*bson.D{
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
		{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5})}},
	}

	targetDatabase := NewMockDatabase(targetData)

	SynchronizeCollection(context.TODO(), 1, source, targetDatabase, targetDatabase, "test", "test")

	for _, item := range targetDatabase.Items {
		id, _ := getObjectId(item)
		log.Println("Item: ", id)
	}
}
