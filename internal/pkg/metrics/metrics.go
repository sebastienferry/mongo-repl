package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	NoOp      = "n"  // NoOp operation
	DbOp      = "db" // Database operation
	UpdateOp  = "u"  // Update operation
	DeleteOp  = "d"  // Delete operation
	InsertOp  = "i"  // Insert operation
	CommandOp = "c"  // Command operation
)

var (
	// Define a custom registry
	Registry *prometheus.Registry

	SnapshotProgressGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongo_repl_full_sync_progress",
		Help: "The progress of the full sync",
	}, []string{"database", "collection"})

	SnapshotReadCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_read_total",
		Help: "The total number of documents fetched during a full sync",
	}, []string{"database", "collection"})

	SnapshotWriteCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_write_total",
		Help: "The total number of documents written during a full sync",
	}, []string{"database", "collection", "operation"})

	SnapshotErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_error_total",
		Help: "The total number of documents written during a full sync",
	}, []string{"database", "collection", "error"})

	IncrSyncOplogReadCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_incr_sync_oplog_read_total",
		Help: "The total number of documents fetched during an incremental sync",
	}, []string{"database", "collection", "operation"})

	IncrSyncOplogWriteCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_incr_sync_oplog_write_total",
		Help: "The total number of documents written during an incremental sync",
	}, []string{"database", "collection", "operation"})

	CheckpointGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mongo_repl_incr_sync_checkpoint",
		Help: "The checkpoint of the incremental sync",
	})

	MongoReplSourceTotalDocumentCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongo_repl_total_document_count",
		Help: "The total number of documents in the source database",
	}, []string{"origin", "database", "collection"})
)

func init() {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(SnapshotProgressGauge)
	Registry.MustRegister(SnapshotReadCounter)
	Registry.MustRegister(SnapshotWriteCounter)
	Registry.MustRegister(SnapshotErrorTotal)
	Registry.MustRegister(IncrSyncOplogReadCounter)
	Registry.MustRegister(IncrSyncOplogWriteCounter)
	Registry.MustRegister(CheckpointGauge)
	Registry.MustRegister(MongoReplSourceTotalDocumentCount)
}
