package mong

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mong struct {
	// Keep the connection/client
	Client *mongo.Client
}

// NewMongo returns a new Mongo struct
func NewMongo(connectUri string) *Mong {

	c, err := connect(context.TODO(), connectUri)
	if err != nil {
		log.Fatal("Error connecting to the server: ", err)
	}

	log.Info("Successfully connected to the server", config.ObfuscateCrendentials(connectUri))

	mongo := &Mong{
		Client: c,
	}

	return mongo
}

// Connect to the MongoDB server
func connect(ctx context.Context, connectUri string) (*mongo.Client, error) {

	// Create a new client and connect to the source server
	connectOpts := options.Client().ApplyURI(connectUri)
	client, err := mongo.Connect(ctx, connectOpts)
	if err != nil {
		log.Error("Error connecting to the source server: ", err)
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
