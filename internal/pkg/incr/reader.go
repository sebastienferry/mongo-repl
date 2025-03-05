package incr

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/api"
	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/collections"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/filters"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"github.com/sebastienferry/mongo-repl/internal/pkg/snapshot"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CursorWaitTime = 5 * time.Second
)

type Reader interface {
	StartReader(context.Context)
	StopReader(context.Context)
}

type OplogReader struct {
	ckpt      checkpoint.CheckpointManager
	filter    *filters.Filter
	latest    primitive.Timestamp
	queue     chan *oplog.ChangeLog
	options   *options.FindOptions
	cmdc      <-chan commands.Command
	done      chan bool
	state     int
	snapshots *collections.AtomicQueue[api.SnapshotRequest]
}

func NewOplogReader(ckpt checkpoint.CheckpointManager,
	latest primitive.Timestamp,
	cmdc <-chan commands.Command,
	queue chan *oplog.ChangeLog) *OplogReader {
	return &OplogReader{
		latest:    latest,
		ckpt:      ckpt,
		filter:    filters.NewFilter(),
		options:   options.Find(),
		queue:     queue,
		cmdc:      cmdc,
		done:      make(chan bool),
		state:     StateUnknown,
		snapshots: collections.NewAtomicQueue[api.SnapshotRequest](),
	}
}

func (r *OplogReader) StartReader(ctx context.Context) {
	go r.RunReader(ctx)
}

func (r *OplogReader) RunReader(ctx context.Context) {

	// Set options
	r.options.SetBatchSize(int32(8192))
	//r.findOptions.SetNoCursorTimeout(true)
	//r.findOptions.SetCursorType(options.Tailable)
	//r.findOptions.SetSort(bson.D{{"$natural", 1}})

	// Read forever
	r.state = StateRunning
	for {

		go func() {

			for {
				cmd := <-r.cmdc
				switch cmd.Id {
				case commands.CmdIdPauseIncr:
					r.state = StatePaused
					log.Info("incremental replication paused")
				case commands.CmdIdResumeIncr:
					r.state = StateRunning
					log.Info("incremental replication resumed")
				case commands.CmdIdSnapshot:

					// Extract the collection to snapshot
					if len(cmd.Arguments) <= 1 {
						log.Warn("invalid argument for snapshot")
					}

					database := cmd.Arguments[0]
					collection := cmd.Arguments[1]

					r.snapshots.Enqueue(api.SnapshotRequest{
						Database:   database,
						Collection: collection,
					})
					log.Info("snapshot request received for ", collection)

				case StateRunning:
				default:
				}
			}
		}()

		// Check if we should stop processing
		select {
		case <-r.done:
			log.Info("stopping oplog reader")
			return
		default:
		}

		if r.state == StatePaused {
			time.Sleep(CursorWaitTime)
			log.Debug("incremental replication is paused, sleeping for ", CursorWaitTime.Seconds(), " secs")
			continue
		}

		if !r.snapshots.IsEmpty() {
			// Execute the snapshot for the collection
			// Currenctly this is synchronous to the reader.
			// Shall we have a dedicated go routine to handle this
			// And have this thread to be waiting for it ?
			// Should we store some state (the snapshot queue) in the database ?
			requested := r.snapshots.Dequeue()
			snapshot := snapshot.NewDeltaReplication(
				mdb.NewMongoItemReader(mdb.Registry.GetSource(), requested.Database, requested.Collection),
				mdb.NewMongoItemReader(mdb.Registry.GetTarget(), requested.Database, requested.Collection),
				mdb.NewMongoWriter(mdb.Registry.GetTarget(), requested.Database, requested.Collection),
				requested.Database, requested.Collection, false, config.Current.Repl.Full.BatchSize)

			err := snapshot.SynchronizeCollection(ctx)
			if err != nil {
				log.Error("error during snapshot: ", err)
			}
		}

		// Get the oplog cursor
		filterOnTs := bson.D{{"ts", bson.D{{"$gt", r.latest}}}}
		cur, err := mdb.Registry.GetSource().GetClient(ctx).Database(checkpoint.OplogDatabase).Collection(checkpoint.OplogCollection).Find(nil, filterOnTs, r.options)
		if err != nil {
			log.Error("error getting oplog cursor: ", err)
			time.Sleep(CursorWaitTime)
			continue
		}

		for cur.Next(context.Background()) {

			if err := cur.Err(); err != nil {
				log.Error("error getting next oplog entry: ", err)
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
				log.Error("error unmarshalling oplog entry: ", err)
				continue
			}

			if !r.filter.KeepOperation(l.Operation) {
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
				command, found := mdb.ExtraCommandName(l.Object)
				if found && filters.KeepOperation(command) {

					cmd := l.Object
					computedCmd := primitive.D{}
					computedCmdSize := 0

					// A command is a map of sub-commands
					for _, ele := range cmd {
						switch ele.Key {

						// ApplyOps is a special command that contains a list of sub-commands
						// We should filter out the unwanted sub-commands on the operation and namespace
						case ApplyOps:
							computedCmd, computedCmdSize = SanitizeApplyOps(ele, KeepSubOp, computedCmd, computedCmdSize)
						case "startIndexBuild":
						case "indexBuildUUID":
						case "index":
						case "indexes":
							continue
						case "commitIndexBuild":
						case "dropIndexes":
							computedCmd = cmd
						default:
							log.Info("unknown command: ", ele.Key)
							jsonCmd, _ := json.Marshal(l)
							log.Info("command: " + string(jsonCmd))
						}
					}

					if computedCmdSize > 0 {
						// Replace the command with the filtered one
						l.Object = computedCmd
						r.queue <- &oplog.ChangeLog{
							ParsedLog:  l,
							Db:         db,
							Collection: coll,
						}

						// Only increment the counter if we have sanitized sub-commands
						// TODO: Should we increment by the number of sub-commands?
						metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
					}

					// Always update the checkpoint to advance in the oplog
					r.latest = l.Timestamp

				} else {
					// We are not interested in this command
					// Yet we still need to update the checkpoint
					// TODO: Check if we need to update the checkpoint
					log.Debug("unwanted command: ", command)
					continue
				}

			} else {
				// Get the database and collection
				db, coll = oplog.GetDbAndCollection(l.Namespace)

				// Check if we should replicate the command
				if !r.filter.KeepCollection(db, coll) {
					continue
				}

				// Process the oplog entry
				r.queue <- &oplog.ChangeLog{
					ParsedLog:  l,
					Db:         db,
					Collection: coll,
				}
				r.latest = l.Timestamp
				metrics.IncrSyncOplogReadCounter.WithLabelValues(db, coll, l.Operation).Inc()
			}
		}

		// Release the cursor
		cur.Close(context.Background())
		time.Sleep(CursorWaitTime)
	}
}

func (o *OplogReader) StopReader() {
	o.done <- true
}
