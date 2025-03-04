package mdb

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MDB struct {
	Uri    string
	client *mongo.Client
}

// NewMongo returns a new Mongo struct
func NewMongo(uri string) *MDB {
	return &MDB{
		Uri:    uri,
		client: nil,
	}
}

func (mdb *MDB) GetClient(ctx context.Context) *mongo.Client {
	if mdb.client != nil && mdb.isOk(ctx) {
		return mdb.client
	}
	client, err := mdb.connect(ctx)
	if err != nil {
		log.Fatal("error connecting to the server: ", err)
	}
	log.Info("successfully connected to the server", config.ObfuscateCrendentials(mdb.Uri))
	mdb.client = client
	return mdb.client
}

func (conn *MDB) isOk(ctx context.Context) bool {
	if err := conn.client.Ping(ctx, nil); err != nil {
		return false
	}
	return true
}

// Connect to the MongoDB server
func (mdb *MDB) connect(ctx context.Context) (*mongo.Client, error) {

	// Create a new client and connect to the source server
	connectOpts := options.Client().ApplyURI(mdb.Uri)
	client, err := mongo.Connect(ctx, connectOpts)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// IsDuplicateKeyError checks if the error is a duplicate key error
func IsDuplicateKeyError(err error) bool {
	// handles SERVER-7164 and SERVER-11493
	for ; err != nil; err = unwrap(err) {
		if e, ok := err.(mongo.ServerError); ok {
			return e.HasErrorCode(11000) || e.HasErrorCode(11001) || e.HasErrorCode(12582) ||
				e.HasErrorCodeWithMessage(16460, " E11000 ")
		}
	}
	return false
}

// unwrap the error
func unwrap(err error) error {
	u, ok := err.(interface {
		Unwrap() error
	})
	if !ok {
		return nil
	}
	return u.Unwrap()
}

func ExtractId(doc bson.Raw) primitive.E {
	var data bson.D
	if err := bson.Unmarshal(doc, &data); err == nil {
		for _, bsonE := range data {
			if bsonE.Key == "_id" {
				return bsonE
			}
		}
	}
	return primitive.E{}
}
