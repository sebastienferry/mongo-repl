package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sebastienferry/mongo-repl/internal/pkg/api"
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"github.com/sebastienferry/mongo-repl/internal/pkg/repl"
	logrus "github.com/sirupsen/logrus"
)

func main() {

	// Load the configuration
	err := config.Current.LoadConfig()
	if err != nil {
		log.Fatal("Error loading configuration: ", err)
	}
	log.Debug("Configuration loaded")
	config.Current.LogConfig()

	// Logger initiatilization
	level := log.FromString(config.Current.Logging.Level)
	log.SetLogLevel(level)
	log.SetLogFormatter(&logrus.TextFormatter{
		FullTimestamp: false,
		DisableColors: false,
	})
	log.Debug("Starting mongo-repl")
	log.Debug(fmt.Sprintf("log level: %d (%s)", level, config.Current.Logging.Level))

	// Setup mongodb connectivity
	mdb.Registry = mdb.NewMongoRegistry(config.Current)

	// Start the API server
	go api.StartApi()

	// Start the replication
	go repl.StartReplication(context.Background())

	// Prepare to handle SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Shutdown
	// TODO: Pass some context to API and replication to gracefully shutdown
	log.Info("Shutting down")
}
