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
		log.Info("pause command sent")
		c.String(200, "Toto")
	default:
		log.Info("pause command not sent")
		// Probably due to too much commands enqueued
		c.String(429, "Toto")
	}
}

func (a *CommandApi) ResumeIncrReplication(c *gin.Context) {

	// Resume the incremental replication
	select {
	case a.commands <- commands.CmdResumeIncremental:
		log.Info("resume command sent")
		c.String(200, "Toto")
	default:
		log.Info("resume command not sent")
		// Probably due to too much commands enqueued
		c.String(429, "Toto")
	}
}

type SnapshotRequest struct {
	Database   string `json:"database" binding:"required"`
	Collection string `json:"collection" binding:"required"`
}

func (a *CommandApi) RunSnapshot(c *gin.Context) {

	var snapshops []SnapshotRequest
	if err := c.ShouldBindBodyWithJSON(&snapshops); err != nil || len(snapshops) <= 0 {
		log.ErrorWithFields("error when triggering snapshop",
			log.Fields{"collection": "collection", "error": err})

		c.String(500, "Toto")
		return
	}

	success := true
	for _, snapshot := range snapshops {
		select {
		case a.commands <- commands.NewCmdSnapshot(snapshot.Database, snapshot.Collection):
			log.InfoWithFields("snaphot command sent",
				log.Fields{"database": snapshot.Database, "collection": snapshot.Collection})
		default:
			log.Error("snaphot command not sent")
			success = false
		}
	}

	if success {
		c.String(200, "Toto")
	} else {
		c.String(429, "Toto")
	}
}
