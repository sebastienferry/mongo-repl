package mdb

import (
	"fmt"
	"strings"

	"github.com/sebastienferry/mongo-repl/internal/pkg/filters"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CommandOperation struct {
	concernSyncData bool
	runOnAdmin      bool // some commands like `renameCollection` need run on admin database
	needFilter      bool // should be ignored in shake
}

func TryGetObjectId(document primitive.D) (primitive.ObjectID, bool) {
	for _, elem := range document {
		if elem.Key == "_id" {
			if oid, ok := elem.Value.(primitive.ObjectID); ok {
				return oid, true
			}
		}
	}
	return primitive.ObjectID{}, false
}

func GetObjectId(document primitive.D) primitive.ObjectID {
	for _, elem := range document {
		if elem.Key == "_id" {
			if oid, ok := elem.Value.(primitive.ObjectID); ok {
				return oid
			}
		}
	}
	return primitive.ObjectID{}
}

func FindFiledPrefix(input bson.D, prefix string) bool {
	for id := range input {
		if strings.HasPrefix(input[id].Key, prefix) {
			return true
		}
	}

	return false
}

// pay attention: the input bson.D will be modified.
func RemoveField(input bson.D, key string) bson.D {
	flag := -1
	for id := range input {
		if input[id].Key == key {
			flag = id
			break
		}
	}

	if flag != -1 {
		input = append(input[:flag], input[flag+1:]...)
	}
	return input
}

// "o" : { "$v" : 2, "diff" : { "d" : { "count" : false }, "u" : { "name" : "orange" }, "i" : { "c" : 11 } } }
func DiffUpdateOplogToNormal(updateObj bson.D) (interface{}, error) {

	diffObj := GetKey(updateObj, "diff")
	if diffObj == nil {
		return updateObj, fmt.Errorf("don't have diff field updateObj:[%v]", updateObj)
	}

	bsonDiffObj, ok := diffObj.(bson.D)
	if !ok {
		return updateObj, fmt.Errorf("diff field is not bson.D updateObj:[%v]", updateObj)
	}

	result, err := BuildUpdateDelteOplog("", bsonDiffObj)
	if err != nil {
		return updateObj, fmt.Errorf("parse diffOplog failed updateObj:[%v] err[%v]", updateObj, err)
	}

	return result, nil

}

func GetKey(log bson.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(log, wanted)
	return ret
}

func GetKeyWithIndex(log bson.D, wanted string) (interface{}, int) {
	if wanted == "" {
		wanted = "_id"
	}

	// "_id" is always the first field
	for id, ele := range log {
		if ele.Key == wanted {
			return ele.Value, id
		}
	}

	return nil, 0
}

func GetId(log bson.D) (primitive.ObjectID, error) {
	oid := GetKey(log, "_id")
	if oid == nil {
		return primitive.ObjectID{}, fmt.Errorf("No ObjectID found")
	}
	return oid.(primitive.ObjectID), nil
}

func BuildUpdateDelteOplog(prefixField string, obj bson.D) (interface{}, error) {
	var result bson.D

	for _, ele := range obj {
		if ele.Key == "d" {
			result = append(result, primitive.E{
				Key:   "$unset",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if ele.Key == "i" || ele.Key == "u" {
			result = append(result, primitive.E{
				Key:   "$set",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if len(ele.Key) > 1 && ele.Key[0] == 's' {
			// s means subgroup field(array or nest)
			tmpPrefixField := ""
			if len(prefixField) == 0 {
				tmpPrefixField = ele.Key[1:]
			} else {
				tmpPrefixField = prefixField + "." + ele.Key[1:]
			}

			nestObj, err := BuildUpdateDelteOplog(tmpPrefixField, ele.Value.(bson.D))
			if err != nil {
				return obj, fmt.Errorf("parse ele[%v] failed, updateObj:[%v]", ele, obj)
			}
			if _, ok := nestObj.(mongo.Pipeline); ok {
				return nestObj, nil
			} else if _, ok := nestObj.(bson.D); ok {
				for _, nestObjEle := range nestObj.(bson.D) {
					result = append(result, nestObjEle)
				}
			} else {
				return obj, fmt.Errorf("unknown nest type ele[%v] updateObj:[%v] nestObj[%v]", ele, obj, nestObj)
			}

		} else if len(ele.Key) > 1 && ele.Key[0] == 'u' {
			result = append(result, primitive.E{
				Key: "$set",
				Value: bson.D{
					primitive.E{
						Key:   prefixField + "." + ele.Key[1:],
						Value: ele.Value,
					},
				},
			})

		} else if ele.Key == "l" {
			if len(result) != 0 {
				return obj, fmt.Errorf("len should be 0, Key[%v] updateObj:[%v], result:[%v]",
					ele, obj, result)
			}

			return mongo.Pipeline{
				{{"$set", bson.D{
					{prefixField, bson.D{
						{"$slice", []interface{}{"$" + prefixField, ele.Value}},
					}},
				}}},
			}, nil

		} else if ele.Key == "a" && ele.Value == true {
			continue
		} else {
			return obj, fmt.Errorf("unknow Key[%v] updateObj:[%v]", ele, obj)
		}
	}

	return result, nil
}

func combinePrefixField(prefixField string, obj interface{}) interface{} {
	if len(prefixField) == 0 {
		return obj
	}

	tmpObj, ok := obj.(bson.D)
	if !ok {
		return obj
	}

	var result bson.D
	for _, ele := range tmpObj {
		result = append(result, primitive.E{
			Key:   prefixField + "." + ele.Key,
			Value: ele.Value})
	}

	return result
}

func ExtraCommandName(o bson.D) (string, bool) {
	// command name must be at the first position
	if len(o) > 0 {
		if _, exist := filters.AllowedOperation[o[0].Key]; exist {
			return o[0].Key, true
		}
	}

	return "", false
}

func ApplyOpsFilter(key string) bool {
	// convert to map if has more later
	k := strings.TrimSpace(key)
	if k == "$db" {
		// 40621, $db is not allowed in OP_QUERY requests
		return true
	} else if k == "ui" {
		return true
	}

	return false
}

func ComputeIdsWindow(boundaries ...primitive.ObjectID) (primitive.ObjectID, primitive.ObjectID) {
	first := primitive.ObjectID{}
	last := primitive.ObjectID{}
	if len(boundaries) > 0 {
		first = boundaries[0]
	}
	if len(boundaries) > 1 {
		last = boundaries[1]
	}
	return first, last
}
