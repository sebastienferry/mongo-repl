# Mongo-Repl - Mongo OPLog

The MongoDB OPLog is a sequential list of operations that can read from the `local.oplog.rs` namespace. The sequence is guaranteed to be in the same order than the execution of operations executed on a replica set. to record and replay the OPLog. This is what is used in `Mongo-Repl` to replicate data.

## OPLog entry

An OPLog entry has the following format:

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
