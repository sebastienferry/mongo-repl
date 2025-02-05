package mdb

import (
	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
)

type MongoRegistry struct {
	source *MDB
	target *MDB
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

func (m *MongoRegistry) GetSource() *MDB {

	if m.source == nil {
		log.Fatal("source is nil")
	}
	return m.source
}

func (m *MongoRegistry) GetTarget() *MDB {

	if m.target == nil {
		log.Fatal("target is nil")
	}
	return m.target
}
