package snapshot

import (
	"bytes"
	"context"
	"fmt"
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
		Items: slices.Clone(items),
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

func CreateTestData(items ...byte) []*bson.D {

	var data []*bson.D
	for _, i := range items {
		data = append(data, &bson.D{{"_id", primitive.ObjectID([12]byte{
			// Convert the int to a 12 bytes array
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i})}})
	}

	return data
}

func TestCompareAndSync(t *testing.T) {

	var data = []struct {
		source   []*bson.D
		target   []*bson.D
		expected int
	}{
		{
			// Both source and target are nil
			nil, nil, 0,
		},
		{
			// Both source and target are empty
			[]*bson.D{}, []*bson.D{}, 0,
		},
		{
			// Only source has data: Add the all
			CreateTestData(1, 2, 3, 4), nil, 4,
		},
		{
			// Only target has data : Drop them all
			nil, CreateTestData(1, 2, 3, 4), 0,
		},
		{
			// Source and target are the same : Update them all
			CreateTestData(1, 2, 3, 4), CreateTestData(1, 2, 3, 4), 4,
		},
		{
			// Source has less data than target : Remove the extra
			CreateTestData(1, 3), CreateTestData(1, 2, 3, 4), 2,
		},
		{
			// Source has more data than target : Add the missing
			// 3 is updated, 8, 9, 12 are added and 4, 5 are removed
			CreateTestData(1, 3, 8, 9, 12), CreateTestData(2, 3, 4, 5), 5,
		},
	}

	for _, d := range data {
		synchronizer := NewMockDatabase(d.target)
		compareAndSync(context.TODO(), d.source, d.target, synchronizer)

		// Check if the data was inserted
		if len(synchronizer.Items) != d.expected {
			t.Errorf(fmt.Sprintf("Expected %d item, got", d.expected), len(synchronizer.Items))
		}
	}
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

	source := NewMockDatabase(CreateTestData(1, 2, 4))
	targetData := CreateTestData(1, 3, 4, 5)
	targetDatabase := NewMockDatabase(targetData)

	SynchronizeCollection(context.TODO(), 1, source, targetDatabase, targetDatabase, "test", "test")
	for _, item := range targetDatabase.Items {
		id, _ := getObjectId(item)
		log.Println("Item: ", id)
	}
}
