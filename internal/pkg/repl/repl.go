package repl

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/incr"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
	"github.com/sebastienferry/mongo-repl/internal/pkg/snapshot"
	"github.com/sebastienferry/mongo-repl/internal/pkg/stats"
)

const (
	UnknownReplState     = iota
	InitialReplState     = 1
	IncrementalReplState = 2
)

var (
	ReplicationStates = map[int]string{
		UnknownReplState:     "unknown",
		InitialReplState:     "initial",
		IncrementalReplState: "incremental",
	}
)

func StartReplication(ctx context.Context, commands chan commands.Command) {
	go RunReplication(ctx, commands)
}

func RunReplication(ctx context.Context, commands chan commands.Command) {

	log.Info("starting replication")
	checkpointManager := checkpoint.NewMongoCheckpointService(
		config.Current.Repl.Id,
		config.Current.Repl.Incr.State.Database,
		config.Current.Repl.Incr.State.Collection)

	// Establish the list of dbAndCollections to replicate
	dbAndCollections, err := mdb.GetCollections(ctx, config.Current.Repl.Databases)
	if err != nil {
		log.Fatal("error getting the list of collections to replicate: ", err)
	}

	// Start the collections stats monitoring
	stats := stats.NewCollectionStats(dbAndCollections)
	stats.StartCollectionStats(ctx)

	replicationState := UnknownReplState
	for replicationState < IncrementalReplState {

		// Determine the replication state
		ckpt, err := checkpointManager.GetCheckpoint(ctx)
		if err != nil {
			log.Fatal("error getting the checkpoint: ", err)
		}

		metrics.CheckpointGauge.Set(float64(ckpt.LatestTs.T))

		var state int = getReplState(ckpt)
		log.Info("replication state: ", ReplicationStates[state])

		// Start the replication based on the type
		switch state {
		case InitialReplState:
			log.Info("starting full replication")
			// Block until the full replication is done
			snapshot.NewSnapshot(checkpointManager).RunSnapshots(ctx, dbAndCollections)
		case IncrementalReplState:
			log.Info("starting incremental replication")
			// Run the incremental replication, blocking here
			incr.NewIncr(checkpointManager, commands).RunIncremental(ctx)
		default:
			log.Fatal("unknown replication type")
		}
	}
}

// Check the replication state to determine if
// a full document replication is needed of if we can proceed
// with the incremental replication based on the oplog.
func getReplState(ckpt checkpoint.Checkpoint) int {
	// Check the replication state
	log.Info("checking replication state")

	// We start with an unknown replication state
	foundReplType := UnknownReplState

	var lastLsnSync int64 = ckpt.LatestLSN
	if lastLsnSync == 0 {
		log.Info("no previous replication state found")
		foundReplType = InitialReplState
	} else {

		// We need to chech if the last LSN synched on the target
		// is included in the oplog of the source. Otherwise we need
		// to perform a full replication.
		log.Info("last LSN synched: ", lastLsnSync)
		foundReplType = IncrementalReplState
	}
	return foundReplType
}
