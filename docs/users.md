# MongoRepl - Users

To operate, you need to have a user that has the following rights:

```
use("<auth_db>");
db.createUser({
    user: "mongo-repl",
    pwd: "mongo-repl",
    roles: [
        { role: "readWrite", db: "<db_to_replicate>" },
        { role: "read", db: "local" }
    ]
});
```
