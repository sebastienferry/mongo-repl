package snapshot

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ItemsResult struct {
	Items     []*bson.D
	HighestId primitive.ObjectID
	Count     int
}

func (r ItemsResult) IsEmpty() bool {
	return r.Count == 0
}
