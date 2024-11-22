package mong

import "github.com/sebastienferry/mongo-repl/internal/pkg/config"

type MongoRegistry struct {
	source *Mong
	target *Mong
}

func NewMongoRegistry() *MongoRegistry {

	source := NewMongo(config.Current.Repl.Source)
	target := NewMongo(config.Current.Repl.Target)

	return &MongoRegistry{
		source: source,
		target: target,
	}
}

var Registry *MongoRegistry = NewMongoRegistry()

func (m *MongoRegistry) GetSource() *Mong {
	return m.source
}

func (m *MongoRegistry) GetTarget() *Mong {
	return m.target
}
