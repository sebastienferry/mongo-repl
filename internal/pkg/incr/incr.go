package incr

import (
	"context"
	"log"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
)

func StartIncrementalReplication(ctx context.Context, checkpointManager checkpoint.CheckpointManager) {

	checkpoint, err := checkpointManager.GetCheckpoint(ctx)
	if err != nil {
		log.Fatal("Error getting the checkpoint: ", err)
	}

	// Read the oplog
	reader := NewOplogReader(checkpoint)
	reader.StartReader()
}

func parseOplog() {

}
