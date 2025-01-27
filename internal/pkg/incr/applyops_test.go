package incr

import (
	"testing"

	"github.com/sebastienferry/mongo-repl/internal/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestFilterApplyOps(t *testing.T) {

	// Define some configuration
	config.Current = &config.AppConfig{
		Repl: config.ReplConfig{
			DatabasesIn: map[string]bool{
				"db1": true,
			},
			FiltersIn: map[string]bool{
				"coll1": true,
				"coll2": true,
			},
			FiltersOut: map[string]bool{
				"coll3": true,
			},
		},
	}

	// Define a set of applyops operations
	applyOpsDoc := []bson.E{bson.E{Key: "applyOps", Value: bson.A{
		// In
		bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "db1.coll1"}, {Key: "ui", Value: "1"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 1}},
		bson.D{{Key: "op", Value: "u"}, {Key: "ns", Value: "db1.coll2"}, {Key: "ui", Value: "2"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 2}},
		bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "db1.coll1"}, {Key: "ui", Value: "3"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 3}},

		//Out
		bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: ""}, {Key: "ui", Value: "4"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 4}},
		bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "db2.coll1"}, {Key: "ui", Value: "3"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 3}},
		bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "db1.coll3"}, {Key: "ui", Value: "3"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 3}},
	}}}

	computedCmd := primitive.D{}
	computedCmdSize := 0

	for _, ele := range applyOpsDoc {

		switch ele.Key {
		case "applyOps":
			// Filter out unwanted sub-commands
			computedCmd, computedCmdSize = FilterApplyOps(ele, KeepSubOp, bson.D{}, computedCmdSize)
		}

	}

	if computedCmdSize != 3 {
		t.Errorf("FilterApplyOps() = %d; want 3", computedCmdSize)
	}

	if len(computedCmd[0].Value.(bson.A)) != 3 {
		t.Errorf("FilterApplyOps() = %d; want 1", len(computedCmd))
	}
}

type FilterSubOpsTestCase struct {
	Doc     bson.D
	Allowed bool
}

func TestFilterSubOps(t *testing.T) {

	// Define some configuration
	config.Current = &config.AppConfig{
		Repl: config.ReplConfig{
			DatabasesIn: map[string]bool{
				"db1": true,
			},
		},
	}

	// Define a set of applyops operations
	applyOpsDoc := []FilterSubOpsTestCase{

		// Allowed
		{Doc: bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "db1.coll1"}, {Key: "ui", Value: "1"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 1}}, Allowed: true},
		{Doc: bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "db1.coll1"}, {Key: "ui", Value: "1"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 1}}, Allowed: true},
		{Doc: bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "db1.coll1"}, {Key: "ui", Value: "1"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 1}}, Allowed: true},

		// Not allowed
		{Doc: bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: ""}, {Key: "ui", Value: "4"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 4}}, Allowed: false},
		{Doc: bson.D{{Key: "op", Value: ""}, {Key: "ns", Value: "db2.coll1"}, {Key: "ui", Value: "3"}, {Key: "o", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "ts", Value: 3}}, Allowed: false},
	}

	for _, doc := range applyOpsDoc {

		if KeepSubOp(doc.Doc) != doc.Allowed {
			t.Errorf("FilterSubOps() = false; want true")
		}
	}

}
