package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
)

type CommandApi struct {
	commands chan<- commands.Command
}

func NewCommandApi(commands chan<- commands.Command) *CommandApi {
	return &CommandApi{
		commands: commands,
	}
}

func (a *CommandApi) PauseIncrReplication(c *gin.Context) {

	// Pause the incremental replication
	select {
	case a.commands <- commands.CmdPauseIncremental:
		log.Info("Pause command sent")
		c.Status(200)
	default:
		log.Info("Pause command not sent")
		// Proably due to too much commands enqueued
		c.Status(429)
	}
}

func (a *CommandApi) ResumeIncrReplication(c *gin.Context) {

	// Resume the incremental replication
	select {
	case a.commands <- commands.CmdResumeIncremental:
		log.Info("Resume command sent")
		c.Status(200)
	default:
		log.Info("Resume command not sent")
		// Proably due to too much commands enqueued
		c.Status(429)
	}
}
