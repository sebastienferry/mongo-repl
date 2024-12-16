package incr

import (
	"context"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/filter"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

			filterOnTs := bson.D{{"ts", bson.D{{"$gt", latestTs}}}}
			cur, err := mdb.Registry.GetSource().Client.Database(checkpoint.OplogDatabase).Collection(checkpoint.OplogCollection).Find(nil, filterOnTs, findOptions)
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

				if !o.oplogFilter.KeepOperation(l.Operation) {
					continue
				}

				// Filter out unwanted operations
				var db, coll string
				if l.Operation == oplog.CommandOp {

					// Namespace is not what you think it is for "c" operations
					// It would be "admin.$cmd", the real collection is store in
					// the "ns" field for sub-entries of the command
					db, coll = oplog.GetDbAndCollection(l.Namespace)

					// Filter out unwanted commands
					command, found := ExtraCommandName(l.Object)
					log.Debug("Command: ", command)
					if found && KeepOperation(command) {

						cmd := l.Object
						computedCmd := primitive.D{}
						computedCmdSize := 0

						// A command is a map of sub-commands
						for _, ele := range cmd {
							switch ele.Key {

							// ApplyOps is a special command that contains a list of sub-commands
							// We should filter out the unwanted sub-commands on the operation and namespace
							case ApplyOps:
								computedCmd, computedCmdSize = FilterApplyOps(ele, KeepSubOp, computedCmd, computedCmdSize)
							default:
								log.Info("Unknown command: ", ele.Key)
							}
						}

						if computedCmdSize > 0 {
							// Replace the command with the filtered one
							l.Object = computedCmd
							queuedLogs <- &oplog.ChangeLog{
								ParsedLog:  l,
								Db:         db,
								Collection: coll,
							}

							// Update the checkpoint
							latestTs = l.Timestamp

							// TODO: Should we increment by the number of sub-commands?
							metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
						}

					} else {
						// We are not interested in this command
						// Yet we still need to update the checkpoint
						// TODO: Check if we need to update the checkpoint
						continue
					}

				} else {
					// Get the database and collection
					db, coll = oplog.GetDbAndCollection(l.Namespace)

					// Check if we should replicate the command
					if !o.oplogFilter.KeepCollection(db, coll) {
						continue
					}

					// Process the oplog entry
					queuedLogs <- &oplog.ChangeLog{
						ParsedLog:  l,
						Db:         db,
						Collection: coll,
					}
					latestTs = l.Timestamp
					metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
				}
			}

			// Release the cursor
			cur.Close(context.Background())
			time.Sleep(CursorWaitTime)
		}
	}()
}

func (o *OplogReader) StopReader() {
	o.done <- true
}
