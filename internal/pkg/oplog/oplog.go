package oplog

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	NoOp      = "n"  // NoOp operation
	DbOp      = "db" // Database operation
	UpdateOp  = "u"  // Update operation
	DeleteOp  = "d"  // Delete operation
	InsertOp  = "i"  // Insert operation
	CommandOp = "c"  // Command operation
)

type GenericOplog struct {
	Raw    []byte
	Parsed *ChangeLog
}

// https://github.com/mongodb/mongo/blob/r6.2.0/src/mongo/db/repl/oplog_entry.idl

type ChangeLog struct {
	ParsedLog

	// /*
	//  * Every field subsequent declared is NEVER persistent or
	//  * transfer on network connection. They only be parsed from
	//  * respective logic
	//  */
	// UniqueIndexesUpdates bson.M // generate by CollisionMatrix
	// RawSize              int    // generate by Decorator
	// SourceId             int    // generate by Validator

	// for update operation, the update condition
	Db         string
	Collection string
}

type ParsedLog struct {
	Timestamp     primitive.Timestamp `bson:"ts" json:"ts"`
	Term          *int64              `bson:"t" json:"t"`
	Hash          *int64              `bson:"h" json:"h"`
	Version       int                 `bson:"v" json:"v"`
	Operation     string              `bson:"op" json:"op"`
	Gid           string              `bson:"g,omitempty" json:"g,omitempty"`
	Namespace     string              `bson:"ns" json:"ns"`
	Object        bson.D              `bson:"o" json:"o"`
	Query         bson.D              `bson:"o2" json:"o2"`                                       // update condition
	UniqueIndexes bson.M              `bson:"uk,omitempty" json:"uk,omitempty"`                   //
	LSID          bson.Raw            `bson:"lsid,omitempty" json:"lsid,omitempty"`               // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate,omitempty" json:"fromMigrate,omitempty"` // move chunk
	TxnNumber     *int64              `bson:"txnNumber,omitempty" json:"txnNumber,omitempty"`     // transaction number in session
	DocumentKey   bson.D              `bson:"documentKey,omitempty" json:"documentKey,omitempty"` // exists when source collection is sharded, only including shard key and _id
	PrevOpTime    bson.Raw            `bson:"prevOpTime,omitempty"`
	UI            *primitive.Binary   `bson:"ui,omitempty" json:"ui,omitempty"` // do not enable currently
}

type GenericObject struct {
	// The object id
	Id primitive.ObjectID `bson:"_id" json:"_id omitempty"`
}

// Split the namespace to get the collection name
// namespace = "whatever.$cmd"
func ParseCmd(namespace string) (string, string) {
	return GetDbAndCollection(namespace)
}

// Split the namespace to get the database name
// namespace = "database.collection.bla"
// parts = ["database", "collection.bla"]
func GetDbAndCollection(namespace string) (string, string) {

	if namespace == "" {
		return "", ""
	}

	var parts []string = []string{namespace}
	sep := strings.Index(namespace, ".")
	if sep > 0 {
		parts = []string{namespace[:sep], namespace[sep+1:]}
		return parts[0], parts[1]
	} else {
		return parts[0], ""
	}
}
