package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// Define a custom registry
	Registry *prometheus.Registry

	FullSyncProgressGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongo_repl_full_sync_progress",
		Help: "The progress of the full sync",
	}, []string{"database", "collection"})

	FullSyncReadCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_read_total",
		Help: "The total number of documents fetched during a full sync",
	}, []string{"database", "collection"})

	FullSyncWriteCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_write_total",
		Help: "The total number of documents written during a full sync",
	}, []string{"database", "collection"})

	FullSyncErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_repl_full_sync_documents_error_total",
		Help: "The total number of documents written during a full sync",
	}, []string{"database", "collection", "error"})
)

func init() {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(FullSyncProgressGauge)
	Registry.MustRegister(FullSyncReadCounter)
	Registry.MustRegister(FullSyncWriteCounter)
	Registry.MustRegister(FullSyncErrorTotal)
}
