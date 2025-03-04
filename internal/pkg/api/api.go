package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	health "github.com/hellofresh/health-go/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/metrics"
)

func StartApi(commands chan<- commands.Command) {

	router := gin.Default()
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{})))
	router.GET("/status", gin.WrapH(CreateHealthCheckHandler()))

	// Commands api
	cmdsApi := NewCommandApi(commands)
	router.POST("/command/incr/pause", cmdsApi.PauseIncrReplication)
	router.POST("/command/incr/resume", cmdsApi.ResumeIncrReplication)
	router.POST("/command/snapshot", cmdsApi.RunSnapshot)

	router.Run(":3000")
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
				return mdb.Registry.GetSource().GetClient(ctx).Ping(ctx, nil)
			}},
		health.Config{
			Name: "mongodb-target",
			Check: func(ctx context.Context) error {
				return mdb.Registry.GetTarget().GetClient(ctx).Ping(ctx, nil)
			},
		},
	))
	return h.Handler()
}
