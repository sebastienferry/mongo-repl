package incr

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Incr struct {
	ckpt     checkpoint.CheckpointManager
	latestTs primitive.Timestamp
	queue    chan *oplog.ChangeLog
	cmdc     chan commands.Command
}

func NewIncr(ckptManager checkpoint.CheckpointManager, cmdc chan commands.Command) *Incr {
	return &Incr{
		ckpt:  ckptManager,
		queue: make(chan *oplog.ChangeLog, 1000),
		cmdc:  cmdc,
	}
}

func (o *Incr) RunIncremental(ctx context.Context) {

	// Get the starting timestamp
	startingTimestamp, err := o.ckpt.GetCheckpoint(context.TODO())
	if err != nil {
		log.Fatal("error getting the checkpoint: ", err)
	}

	// Check the starting timestamp is within the boundaries of the oplog
	oplogBoundaries, err := checkpoint.GetReplicasetOplogWindow()
	if err != nil {
		log.Fatal("error computing the last checkpoint: ", err)
	}

	if startingTimestamp.LatestTs.Compare(oplogBoundaries.Oldest) < 0 {
		log.Fatal("the starting timestamp is older than the oldest timestamp in the oplog")
	}

	o.latestTs = checkpoint.FromInt64(startingTimestamp.LatestLSN)

	// Create both the reader and the writer
	writer := NewOplogWriter(o.ckpt, startingTimestamp.LatestLSN, o.queue)
	reader := NewOplogReader(o.ckpt, startingTimestamp.LatestTs, o.cmdc, o.queue)

	// Start the writer, then the reader
	writer.StartWriter(ctx)
	reader.StartReader(ctx)

	// Also, start the checlpoint autosaver
	o.ckpt.StartAutosave(ctx)

	// Waits until a command arrives on the decicated channel
	for {
		select {
		case <-ctx.Done():
			return
			// case cmd := <-o.cmdc:
			// 	switch cmd.Id {
			// 	case commands.CmdIdTerminate:
			// 		log.Info("received termination command")
			// 		ctx.Done()
			// 	}
			// }
		}
	}
}
