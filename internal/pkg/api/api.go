package api

import (
	"context"
	"net/http"
	"time"

	health "github.com/hellofresh/health-go/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
)

func StartApi() {
	http.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	http.Handle("/status", CreateHealthCheckHandler())
	http.ListenAndServe(":3000", nil)
}

func CreateHealthCheckHandler() http.Handler {

	h, _ := health.New(health.WithComponent(health.Component{
		Name:    "mongo-repl",
		Version: "v1.0",
	}), health.WithChecks(
		health.Config{
			Name:      "mongodb-source",
			Timeout:   time.Second * 5,
			SkipOnErr: true,
			Check: func(ctx context.Context) error {
				return mdb.Registry.GetSource().Client.Ping(ctx, nil)
			}},
		health.Config{
			Name: "mongodb-target",
			Check: func(ctx context.Context) error {
				return mdb.Registry.GetTarget().Client.Ping(ctx, nil)
			},
		},
	))
	return h.Handler()
}
