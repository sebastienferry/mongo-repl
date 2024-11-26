package checkpoint

import (
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func IsZero(ts primitive.Timestamp) bool {
	return ts.T == 0 && ts.I == 0
}

func CompareTimestamps(ts1, ts2 primitive.Timestamp) int {

	if ts1.T == ts2.T && ts1.I == ts2.I {
		return 0
	}

	if ts1.T > ts2.T {
		return 1
	} else if ts1.T < ts2.T {
		return -1
	}

	if ts1.I > ts2.I {
		return 1
	}
	return -1
}

func ToInt64(ts primitive.Timestamp) int64 {
	return int64(ts.T)<<32 + int64(ts.I)
}

func FromInt64(i int64) primitive.Timestamp {
	return primitive.Timestamp{uint32(i >> 32), uint32(i)}
}

func ToDate(ts primitive.Timestamp) time.Time {
	return time.Unix(int64(ts.T), int64(ts.I))
}

var (
	MongoTimestampMax = primitive.Timestamp{math.MaxUint32, math.MaxUint32}
	MongoTimestampMin = primitive.Timestamp{0, 0}
)

type TsWindow struct {
	Oldest primitive.Timestamp
	Newest primitive.Timestamp
}
