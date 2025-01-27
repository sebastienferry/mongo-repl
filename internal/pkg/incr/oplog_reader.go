package incr

import (
	"context"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/filter"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mong"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CursorWaitTime = 5 * time.Second
)

type OplogReader struct {
	ckptManager checkpoint.CheckpointManager
	oplogFilter *filter.Filter
	done        chan bool
}

func NewOplogReader(ckptManager checkpoint.CheckpointManager) *OplogReader {
	return &OplogReader{
		ckptManager: ckptManager,
		oplogFilter: filter.NewFilter(),
		done:        make(chan bool),
	}
}

func (o *OplogReader) StartReader(ctx context.Context) {

	// Get the starting timestamp
	startingTimestamp, err := o.ckptManager.GetCheckpoint(context.TODO())
	if err != nil {
		log.Fatal("Error getting the checkpoint: ", err)
	}

	// Check the starting timestamp is within the boundaries of the oplog
	oplogBoundaries, err := checkpoint.GetReplicasetOplogWindow()
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

	queuedLogs := make(chan *oplog.ChangeLog, 1000)
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
			cur, err := mong.Registry.GetSource().Client.Database(checkpoint.OplogDatabase).Collection(checkpoint.OplogCollection).Find(nil, filter, findOptions)
			if err != nil {
				log.Error("Error getting oplog cursor: ", err)
				time.Sleep(CursorWaitTime)
				continue
			}

			for cur.Next(context.Background()) {

				if err := cur.Err(); err != nil {
					log.Error("Error getting next oplog entry: ", err)
					// Release the cursor
					cur.Close(context.Background())
					// Wait a bit
					time.Sleep(1 * time.Second)
					continue
				}

				// Handle the OPLOG entry
				// MongoShake send this to a channel and use a pool of workers to process the oplog entries
				// For now, we will process the oplog entry in the same goroutine

				var bytes []byte = cur.Current

				// Deserialize the oplog entry
				l := oplog.ParsedLog{}

				err := bson.Unmarshal(bytes, &l)
				if err != nil {
					log.Error("Error unmarshalling oplog entry: ", err)
					continue
				}

				// Get the database and collection
				db, coll := oplog.GetDbAndCollection(l.Namespace)

				// Filter out unwanted operations
				if !o.oplogFilter.KeepOperation(l.Operation) || !o.oplogFilter.KeepCollection(db, coll) {
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
				queuedLogs <- &oplog.ChangeLog{
					ParsedLog:  l,
					Db:         db,
					Collection: coll,
				}
				latestTs = l.Timestamp
				metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
			}

			// Release the cursor
			cur.Close(context.Background())
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
