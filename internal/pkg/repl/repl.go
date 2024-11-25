package repl

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/full"
	"github.com/sebastienferry/mongo-repl/internal/pkg/incr"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
)

const (
	UnknownRepl     = iota
	FullRepl        = 1
	IncrementalRepl = 2
)

var (
	ReplicationTypes = map[int]string{
		UnknownRepl:     "Unknown",
		FullRepl:        "Full",
		IncrementalRepl: "Incremental",
	}
)

func StartReplication(ctx context.Context) {

	log.Info("Starting replication")
	checkpointManager := checkpoint.NewMongoCheckpointService(
		config.Current.Repl.Incr.State.Database, config.Current.Repl.Incr.State.Collection)

	// Determine the replication type
	ckpt, err := checkpointManager.GetCheckpoint(ctx)
	if err != nil {
		log.Fatal("Error getting the checkpoint: ", err)
	}

	var replType int = getReplType(ckpt)
	log.Info("Replication type: ", ReplicationTypes[replType])

	// Start the replication based on the type
	switch replType {
	case FullRepl:
		log.Info("Starting full replication")
		full.StartFullReplication(ctx, checkpointManager)
	case IncrementalRepl:
		log.Info("Starting incremental replication")
		incr.StartIncrementalReplication(ctx, checkpointManager)
	default:
		log.Fatal("Unknown replication type")
	}

}

// Check the replication state to determine if
// a full document replication is needed of if we can proceed
// with the incremental replication based on the oplog.
func getReplType(ckpt checkpoint.Checkpoint) int {
	// Check the replication state
	log.Info("Checking replication state")

	// We start with an unknown replication state
	foundReplType := UnknownRepl

	var lastLsnSync int64 = ckpt.LatestTimestamp
	if lastLsnSync == 0 {
		log.Info("No previous replication state found")
		foundReplType = FullRepl
	} else {

		// We need to chech if the last LSN synched on the target
		// is included in the oplog of the source. Otherwise we need
		// to perform a full replication.
		log.Info("Last LSN synched: ", lastLsnSync)
		foundReplType = IncrementalRepl
	}
	return foundReplType
}
