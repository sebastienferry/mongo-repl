package incr

import (
	"context"

	"github.com/sebastienferry/mongo-repl/internal/pkg/checkpoint"
)

func StartIncrementalReplication(ctx context.Context, checkpointManager checkpoint.CheckpointManager) {

	// Create the reader and start it
	reader := NewOplogReader(checkpointManager)
	reader.StartReader(ctx)

	// Also, start the checkpoint autosave
	checkpointManager.StartAutosave(ctx)
}
