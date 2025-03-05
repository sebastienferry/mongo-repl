package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sebastienferry/mongo-repl/internal/pkg/api"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
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
		log.Fatal("error loading configuration: ", err)
	}
	log.Debug("configuration loaded")
	config.Current.LogConfig()

	// Logger initiatilization
	level := log.FromString(config.Current.Logging.Level)
	log.SetLogLevel(level)
	if config.Current.Logging.ApiKey != "" {
		log.SetLogFormatter(&logrus.JSONFormatter{})
		log.AddDataDogHook(config.Current.Logging.Endpoint, config.Current.Logging.ApiKey)
	} else {
		log.SetLogFormatter(&logrus.TextFormatter{
			FullTimestamp: false,
			DisableColors: false,
		})
	}
	log.Debug("starting mongo-repl")
	log.Debug(fmt.Sprintf("log level: %d (%s)", level, config.Current.Logging.Level))

	// Setup mongodb connectivity
	mdb.Registry = mdb.NewMongoRegistry(config.Current)

	// Create a global commands channel
	commands := make(chan commands.Command, 10)

	// Start the replication
	repl.StartReplication(context.Background(), commands)

	// Start the API server
	api.StartApi(commands)

	// Prepare to handle SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Shutdown
	// TODO: Pass some context to API and replication to gracefully shutdown
	log.Info("shutting down")
}
