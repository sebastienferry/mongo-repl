package incr

import (
	"context"
	"strings"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CursorWaitTime = 5 * time.Second
)

var (
	FilteredOperations = map[string]bool{
		"n":  true, // no-op
		"c":  true, // command
		"db": true, // database

		// Keep the following operations
		"u": false, // update
		"d": false, // delete
		"i": false, // insert
	}
)

type GenericOplog struct {
	Raw    []byte
	Parsed *ChangeLog
}

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

type OplogReader struct {
	ckptManager checkpoint.CheckpointManager
	done        chan bool
}

func NewOplogReader(ckptManager checkpoint.CheckpointManager) *OplogReader {
	return &OplogReader{
		ckptManager: ckptManager,
	}
}

func (o *OplogReader) StartReader(ctx context.Context) {

	// Get the starting timestamp
	startingTimestamp, err := o.ckptManager.GetCheckpoint(context.TODO())
	if err != nil {
		log.Fatal("Error getting the checkpoint: ", err)
	}

	// Check the starting timestamp is within the boundaries of the oplog
	oplogBoundaries, err := checkpoint.GetReplicasetOplogBoundaries()
	if err != nil {
		log.Fatal("Error computing the last checkpoint: ", err)
	}

	if startingTimestamp.LatestTs.Compare(oplogBoundaries.Oldest) < 0 {
		log.Fatal("The starting timestamp is older than the oldest timestamp in the oplog")
	}

	findOptions := options.Find()
	findOptions.SetBatchSize(int32(8192))
	//findOptions.SetNoCursorTimeout(true)
	//findOptions.SetCursorType(options.Tailable)
	//findOptions.SetSort(bson.D{{"$natural", 1}})
	latestTs := checkpoint.FromInt64(startingTimestamp.LatestLSN)

	queuedLogs := make(chan *ChangeLog, 1000)
	writer := NewOplogWriter(startingTimestamp.LatestLSN, queuedLogs, o.ckptManager)
	writer.StartWriter(ctx)

	go func() {
		for {

			// Check if we should stop processing
			select {
			case <-o.done:
				log.Info("Stopping oplog reader")
				return
			default:
			}

			// Get the oplog cursor
			filter := bson.D{{"ts", bson.D{{"$gt", latestTs}}}}
			oplog, err := mong.Registry.GetSource().Client.Database(checkpoint.OplogDatabase).Collection(checkpoint.OplogCollection).Find(nil, filter, findOptions)
			if err != nil {
				log.Error("Error getting oplog cursor: ", err)
				time.Sleep(CursorWaitTime)
				continue
			}

			for oplog.Next(context.Background()) {

				if err := oplog.Err(); err != nil {
					log.Error("Error getting next oplog entry: ", err)
					// Release the cursor
					oplog.Close(context.Background())
					// Wait a bit
					time.Sleep(1 * time.Second)
					continue
				}

				// Handle the OPLOG entry
				// MongoShake send this to a channel and use a pool of workers to process the oplog entries
				// For now, we will process the oplog entry in the same goroutine

				var bytes []byte = oplog.Current

				// Deserialize the oplog entry
				l := ParsedLog{}

				err := bson.Unmarshal(bytes, &l)
				if err != nil {
					log.Error("Error unmarshalling oplog entry: ", err)
					continue
				}

				// Filter out unwanted operations
				if keep, ok := FilteredOperations[l.Operation]; ok && !keep {
					continue
				}

				// Filter out unwanted namespaces
				if l.Namespace == "" {
					continue
				}

				// Filter unwanted data
				db, coll := GetDbAndCollection(l.Namespace)
				if !shouldReplicate(db, coll) {
					continue
				}

				// // Try to get the object id
				// var id primitive.ObjectID
				// if config.Current.Logging.Level == log.DebugLevel {
				// 	for _, bsonE := range l.Object {
				// 		if bsonE.Key == "_id" {
				// 			if oid, ok := bsonE.Value.(primitive.ObjectID); ok {
				// 				id = oid
				// 			}
				// 		}
				// 	}
				// }

				// log.DebugWithFields("OPLOG entry: ", log.Fields{
				// 	"ns": l.Namespace,
				// 	"op": l.Operation,
				// 	"ts": l.Timestamp,
				// 	"id": id,
				// })

				// Process the oplog entry
				queuedLogs <- &ChangeLog{
					ParsedLog:  l,
					Db:         db,
					Collection: coll,
				}
				latestTs = l.Timestamp
				metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
			}

			// Release the cursor
			oplog.Close(context.Background())
			time.Sleep(CursorWaitTime)

			// Refresh the latest timestamp from the checkpoint stored in the DB
			// Every minute, we will refresh the latest timestamp from the checkpoint stored in the DB
			// savedTs, err := o.ckptManager.GetCheckpoint(context.Background())
			// if err != nil {
			// 	log.Error("Error getting the latest checkpoint: ", err)
			// } else {
			// 	latestTs = savedTs.LatestTs
			// }
		}
	}()
}

func (o *OplogReader) StopReader() {
	o.done <- true
}

func shouldReplicate(db string, collection string) bool {

	// Check if the database is part of the one we are targeting
	if ok := config.Current.Repl.DatabasesIn[db]; !ok {
		return false
	}

	if len(config.Current.Repl.FiltersIn) > 0 {
		if _, ok := config.Current.Repl.FiltersIn[collection]; !ok {
			return false
		}
		return true
	} else if config.Current.Repl.FiltersOut[collection] {
		return false
	}
	return true
}

// Split the namespace to get the database name
// namespace = "database.collection.bla"
// parts = ["database", "collection.bla"]
func GetDbAndCollection(namespace string) (string, string) {
	var parts []string = []string{namespace}
	sep := strings.Index(namespace, ".")
	if sep > 0 {
		parts = []string{namespace[:sep], namespace[sep+1:]}
	}
	var db string = parts[0]
	var coll string = parts[1]
	return db, coll
}
