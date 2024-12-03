package mdb

import (
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
)

type MongoRegistry struct {
	source *Mong
	target *Mong
}

func NewMongoRegistry(appConfig *config.AppConfig) *MongoRegistry {

	source := NewMongo(appConfig.Repl.Source)
	target := NewMongo(appConfig.Repl.Target)

	return &MongoRegistry{
		source: source,
		target: target,
	}
}

var Registry *MongoRegistry = nil

func (m *MongoRegistry) GetSource() *Mong {

	if m.source == nil {
		log.Fatal("Source is nil")
	}
	return m.source
}

func (m *MongoRegistry) GetTarget() *Mong {

	if m.target == nil {
		log.Fatal("Target is nil")
	}
	return m.target
}
