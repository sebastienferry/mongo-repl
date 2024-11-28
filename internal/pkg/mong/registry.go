package mong

import "github.com/sebastienferry/mongo-repl/internal/pkg/config"

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
	return m.source
}

func (m *MongoRegistry) GetTarget() *Mong {
	return m.target
}
