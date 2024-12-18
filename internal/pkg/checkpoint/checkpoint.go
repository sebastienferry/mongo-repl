package checkpoint

import (
	"fmt"
	"time"

	"github.com/sebastienferry/mongo-repl/internal/pkg/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	OplogCollection = "oplog.rs"
	OplogDatabase   = "local"
	Newest          = -1
	Oldest          = 1
)

type Checkpoint struct {
	//Id              string    `bson:"_id" json:"_id" omitempty`
	Name      string              `bson:"name" json:"name" omitempty`
	SavedAt   time.Time           `bson:"saved" json:"saved" omitempty`
	Latest    time.Time           `bson:"latest" json:"latest" omitempty`
	LatestTs  primitive.Timestamp `bson:"ts" json:"ts" omitempty`
	LatestLSN int64               `bson:"lsn" json:"lsn" omitempty`
}

func GetReplicasetOplogBoundaries() (TsWindow, error) {

	// smallestNew := MongoTimestampMax
	// biggestNew := MongoTimestampMin
	// smallestOld := MongoTimestampMax
	// biggestOld := MongoTimestampMin
	//tsMap := make(map[string]TimestampNode)

	// for _, database := range databases {

	// Get the most recent timestamp from the oplog
	var newest primitive.Timestamp = MongoTimestampMin
	newest, err := getOplogTimestamp(mdb.Registry.GetSource().Client, Newest)
	if err != nil {
		return TsWindow{}, err
	} else if IsZero(newest) {
		return TsWindow{}, fmt.Errorf("illegal newest timestamp == 0")
	}

	var oldest primitive.Timestamp = MongoTimestampMin
	oldest, err = getOplogTimestamp(mdb.Registry.GetSource().Client, Oldest)
	if err != nil {
		return TsWindow{}, err
	}

	// tsMap[database] = TimestampNode{
	// 	Oldest: oldest,
	// 	Newest: newest,
	// }

	return TsWindow{
		Oldest: oldest,
		Newest: newest,
	}, nil

	// if CompareTimestamps(newest, biggestNew) > 0 {
	// 	biggestNew = newest
	// }
	// if CompareTimestamps(newest, smallestNew) < 0 {
	// 	smallestNew = newest
	// }
	// if CompareTimestamps(oldest, biggestOld) > 0 {
	// 	biggestOld = oldest
	// }
	// if CompareTimestamps(oldest, smallestOld) < 0 {
	// 	smallestOld = oldest
	// }
	// }

	//return tsMap, nil
}

// Get the timestamp of the oldest or newest oplog entry
func getOplogTimestamp(client *mongo.Client, sortType int) (primitive.Timestamp, error) {
	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"$natural", sortType}})
	err := client.Database(OplogDatabase).Collection(OplogCollection).FindOne(nil, bson.M{}, opts).Decode(&result)
	if err != nil {
		return MongoTimestampMin, err
	}

	var ts primitive.Timestamp = result["ts"].(primitive.Timestamp)
	return ts, nil
}

// Converts a mongo Timestamp to an int64
func convertTimeStampToInt64(ts primitive.Timestamp) int64 {
	return int64(ts.T)<<32 + int64(ts.I)
}

func Int64ToUnixTimestampSeconds(ts int64) int64 {
	return ts >> 32
}
