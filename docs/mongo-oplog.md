# Mongo-Repl - Mongo OPLog

The MongoDB OPLog is a sequential list of operations that can read from the `local.oplog.rs` namespace. The sequence is guaranteed to be in the same order than the execution of operations executed on a replica set. This is what is used in `Mongo-Repl` to replicate data.

## OPLog entry

An OPLog entry has the following format:

```json
{
    "ts": Timestamp(1633024800, 1),
    "h": NumberLong("1234567890123456789"),
    "v": 2,
    "op": "<operation>",
    "ns": "<target>",
    "o": {
        "_id": ObjectId("507f1f77bcf86cd799439011"),
        "name": "Dr. Smith",
        "specialty": "surgery"
    }
}
```

```json
{
    "ts": Timestamp(1633024800, 1),
    "h": NumberLong("1234567890123456789"),
    "v": 2,
    "op": "i",
    "ns": "animals_clinic.veterinarians",
    "o": {
        "_id": ObjectId("507f1f77bcf86cd799439011"),
        "name": "Dr. Smith",
        "specialty": "surgery"
    }
}
```




In the Mongo terminology, a namespace is the target of the operation. It could a database and a collection
like `animals_clinic.veterinarians` or use some special keywork like `admin.$cmd`

## `runCommand` operations `c`

### `applyOps`

### `startBuildIndex` & `commitIndexBuild``

When creating one or many indexes, the following two oplog entries are created. This correspond to both the `db.Vets.createIndex` and `db.Vets.createIndexex` commands.

```json
{
    "ts": {"T": 1734532466, "I": 2},
    "t": 406,
    "h": null,
    "v": 2,
    "op": "c",
    "ns": "Animals.$cmd",
    "o": [
        {"Key": "startIndexBuild", "Value": "Vets"},
        { "Key": "indexBuildUUID", "Value": {"Subtype": 4, "Data": "zxXEWU1OSyKnpAPrakxiew=="} },
        {
            "Key": "indexes",
            "Value": [
                [
                    {"Key": "v", "Value": 2},
                    {
                        "Key": "key",
                        "Value": [ {"Key": "specialty", "Value": 1} ]
                    },
                    {"Key": "name", "Value": "specialty_1"}
                ]
            ]
        }
    ],
    "o2": null,
    "PrevOpTime": null,
    "ui": {"Subtype": 4, "Data": "AW6d94hVT7CZpQoubwR3FQ=="}
}
```

and

```json
{
    "ts": {"T": 1734532466, "I": 6},
    "t": 406,
    "h": null,
    "v": 2,
    "op": "c",
    "ns": "Animals.$cmd",
    "o": [
        {"Key": "commitIndexBuild", "Value": "Vets"},
        { "Key": "indexBuildUUID", "Value": {"Subtype": 4, "Data": "zxXEWU1OSyKnpAPrakxiew=="} },
        {
            "Key": "indexes",
            "Value": [
                [
                    {"Key": "v", "Value": 2},
                    {
                        "Key": "key",
                        "Value": [ {"Key": "specialty", "Value": 1} ]
                    },
                    {"Key": "name", "Value": "specialty_1"}
                ]
            ]
        }
    ],
    "o2": null,
    "PrevOpTime": null,
    "ui": {"Subtype": 4, "Data": "AW6d94hVT7CZpQoubwR3FQ=="}
}
```




- namespace: $cmd
- command : startBuildIndex
- config map:
    - "startBuildIndex" : "collection"
    - "indexBuildUUID" : UID
    - "indexes" :
        - "v" : 2
        - "key": "index definition"
        - "name" : "name"


