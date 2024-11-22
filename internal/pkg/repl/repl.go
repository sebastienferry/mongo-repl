package repl

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/full"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
)

const (
	UnknownRepl     = iota
	FullRepl        = 1
	IncrementalRepl = 2
)

func StartReplication(ctx context.Context) {
	log.Info("Starting replication")
	replType := getReplType(context.Background())

	// Start the replication based on the type
	switch replType {
	case FullRepl:
		log.Info("Starting full replication")
		full.StartFullReplication(ctx)
	case IncrementalRepl:
		log.Info("Starting incremental replication")
		StartIncrementalReplication(ctx)
	default:
		log.Fatal("Unknown replication type")
	}

	log.Info("Replication type: ", replType)
}

// Check the replication state to determine if
// a full document replication is needed of if we can proceed
// with the incremental replication based on the oplog.
func getReplType(ctx context.Context) int {
	// Check the replication state
	log.Info("Checking replication state")

	// We start with an unknown replication state
	foundReplType := UnknownRepl

	var lastLsnSync int64 = GetLastLsnSynched(ctx)
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
