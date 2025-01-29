package snapshot

import (
	"context"
	"errors"
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

	var source ItemsResult = ItemsResult{
		Items:     nil,
		HighestId: primitive.ObjectID{},
		Count:     0,
	}

	var target ItemsResult = ItemsResult{
		Items:     nil,
		HighestId: primitive.ObjectID{},
		Count:     0,
	}

	synchronizer := NewMockSynchronizer()

	// Insert some data in the source collection
	compareAndSync(context.TODO(), source, target, synchronizer)

	// Check if the data was inserted
	if len(synchronizer.Items) != 1 {
		t.Error("Expected 1 item, got ", len(synchronizer.Items))
	}
}
