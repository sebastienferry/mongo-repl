package snapshot

import (
	"bytes"
	"context"
	"slices"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MockDatabase struct {
	Items []*bson.D
}

func NewMockDatabase(items []*bson.D) *MockDatabase {
	return &MockDatabase{
		Items: slices.Clone(items),
	}
}

func getIndexById(items []*bson.D, id primitive.ObjectID) (int, bool) {
	return slices.BinarySearchFunc(items, id, func(e *primitive.D, t primitive.ObjectID) int {
		val, ok := TryGetObjectId(e)
		if !ok {
			panic("Item does not have an ID")
		}
		return bytes.Compare(val[:], id[:])
	})
}

func (r *MockDatabase) ReadItems(ctx context.Context, batchSize int, boundaries ...primitive.ObjectID) ([]*bson.D, error) {

	if len(boundaries) == 0 {
		return nil, nil
	}

	first, last := getBoundariesIds(boundaries...)

	// Get the index of the first element
	start, found := getIndexById(r.Items, first)

	if found {
		// Exclude the first element
		// To mimic the greater than operator
		start = start + 1
	}

	end := 0
	for i := start + 1; i < len(r.Items); i++ {
		oid, _ := TryGetObjectId(r.Items[i])
		if (i-start) == batchSize || (!last.IsZero() && bytes.Compare(oid[:], last[:]) > 0) {
			end = i
			break
		}
	}

	if end == 0 {
		end = len(r.Items)
	}

	return r.Items[start:end], nil
}

func (s *MockDatabase) Insert(ctx context.Context, item *primitive.D) error {

	oid := GetObjectId(item)
	index := slices.IndexFunc(s.Items, func(i *bson.D) bool {
		val, ok := TryGetObjectId(i)
		if !ok {
			panic("Item does not have an ID")
		}
		return bytes.Compare(val[:], oid[:]) == 0
	})

	if index >= 0 {
		// Item already exists
		// Do an upsert
		s.Items[index] = item
		return nil
	}

	s.Items = append(s.Items, item)
	slices.SortFunc(s.Items, func(i, j *bson.D) int {
		ii, _ := TryGetObjectId(i)
		jj, _ := TryGetObjectId(j)
		return bytes.Compare(ii[:], jj[:])
	})
	return nil
}

func (s *MockDatabase) InsertMany(ctx context.Context, items []*bson.D) (BulkResult, error) {

	var result = BulkResult{}
	for _, item := range items {
		s.Insert(ctx, item)
		result.InsertedCount++
	}
	return result, nil
}

func (s *MockDatabase) Update(ctx context.Context, source *primitive.D, target *primitive.D) error {
	return nil
}

func (s *MockDatabase) UpdateMany(ctx context.Context, items []*bson.D) (BulkResult, error) {

	var result = BulkResult{}
	for _, item := range items {
		s.Update(ctx, item, item)
		result.UpdatedCount++
	}
	return result, nil
}

func (s *MockDatabase) Delete(ctx context.Context, id primitive.ObjectID) error {

	index := slices.IndexFunc(s.Items, func(i *bson.D) bool {
		val, ok := TryGetObjectId(i)
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

func (s *MockDatabase) DeleteMany(ctx context.Context, ids []primitive.ObjectID) (BulkResult, error) {

	var result = BulkResult{}
	for _, item := range ids {
		s.Delete(ctx, item)
		result.DeletedCount++
	}
	return result, nil
}

func (s *MockDatabase) WriteMany(ctx context.Context, items []*bson.D) (BulkResult, error) {
	return BulkResult{}, nil
}
