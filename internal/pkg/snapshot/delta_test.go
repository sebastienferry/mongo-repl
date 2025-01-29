package snapshot

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MockSynchronizer struct {
	Items map[primitive.ObjectID]*bson.D
}

func NewMockSynchronizer() *MockSynchronizer {
	return &MockSynchronizer{
		Items: make(map[primitive.ObjectID]*bson.D),
	}
}

func (s *MockSynchronizer) Insert(ctx context.Context, item *primitive.D) error {

	id, ok := getObjectId(item)
	if !ok {
		return errors.New("Item does not have an ID")
	}

	s.Items[id] = item
	return nil
}

func (s *MockSynchronizer) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {

	sid, ok := getObjectId(source)
	if !ok {
		return errors.New("Source does not have an ID")
	}

	s.Items[sid] = target
	return nil
}

func (s *MockSynchronizer) Delete(ctx context.Context, item *primitive.D) error {

	id, ok := getObjectId(item)
	if !ok {
		return errors.New("Item does not have an ID")
	}

	delete(s.Items, id)
	return nil
}

func TestCompareAndSync(t *testing.T) {

	var data = []struct {
		source   ItemsResult
		target   ItemsResult
		expected int
	}{
		{
			ItemsResult{
				Items:     nil,
				HighestId: primitive.ObjectID{},
				Count:     0,
			},
			ItemsResult{
				Items:     nil,
				HighestId: primitive.ObjectID{},
				Count:     0,
			},
			0,
		},
		{
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
				Count:     4,
			},
			ItemsResult{
				Items:     nil,
				HighestId: primitive.ObjectID{},
				Count:     0,
			},
			4,
		},
		{
			ItemsResult{
				Items:     nil,
				HighestId: primitive.ObjectID{},
				Count:     0,
			},
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
				Count:     4,
			},
			0,
		},
		{
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
				Count:     4,
			},
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
				Count:     4,
			},
			4,
		},
		{
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}),
				Count:     2,
			},
			ItemsResult{
				Items: []*bson.D{
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})}},
					{{"_id", primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4})}},
				},
				HighestId: primitive.ObjectID([12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}),
				Count:     4,
			},
			2,
		},
	}

	synchronizer := NewMockSynchronizer()

	for _, d := range data {
		compareAndSync(context.TODO(), d.source, d.target, synchronizer)

		// Check if the data was inserted
		if len(synchronizer.Items) != d.expected {
			t.Errorf(fmt.Sprintf("Expected %d item, got", d.expected), len(synchronizer.Items))
		}
	}
}
