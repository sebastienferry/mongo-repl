package incr

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Create indexes on a collection using the createIndexes command
func RunCommandCreateIndexes(database string, l *oplog.ChangeLog, client *mongo.Client) error {

	log.DebugWithFields("execute DDL command", log.Fields{"command": "createIndexes"})

	var collection string
	var indexes bson.A
	for _, ele := range l.Object {

		if ele.Key == "commitIndexBuild" {
			collection = ele.Value.(string)
		}

		if ele.Key == "indexes" {
			for _, index := range ele.Value.(bson.A) {
				indexes = append(indexes, index)
			}
		}
	}

	cmd := bson.D{
		{"createIndexes", collection},
		{"indexes", indexes},
	}

	return client.Database(database).RunCommand(context.TODO(), cmd).Err()
}

func RunCommandDropIndexes(database string, l *oplog.ChangeLog, client *mongo.Client) error {

	log.DebugWithFields("execute DDL command", log.Fields{"command": "dropIndexes"})

	var collection string
	var index string
	for _, ele := range l.Object {

		if ele.Key == "dropIndexes" {
			collection = ele.Value.(string)
		}

		if ele.Key == "index" {
			index = ele.Value.(string)
		}
	}

	cmd := bson.D{
		{"dropIndexes", collection},
		{"index", index},
	}

	return client.Database(database).RunCommand(context.TODO(), cmd).Err()

}

// See this documentation for a full list of available commands:
// https://www.mongodb.com/docs/manual/reference/command/
func RunCommand(database, command string, l *oplog.ChangeLog, client *mongo.Client) error {

	switch command {
	case "applyOps":
		return RunCommandApplyOps(database, l, client)
	case "commitIndexBuild":
		return RunCommandCreateIndexes(database, l, client)
	case "dropIndexes":
		return RunCommandDropIndexes(database, l, client)
	default:
		log.DebugWithFields("unkknow command", log.Fields{"command": command})
	}
	return nil
}
