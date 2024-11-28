# Compute the parent directory
current_dir=$(pwd)
parent_dir=$(dirname "$current_dir")

# Run the docker container
# Expose the port 3000 for prometheus and healthcheck
# Mount the configuration file in the container
# Load the environment variables from the .env file
# - CONFIG_FILE_PATH: /app/conf.yaml
# - SOURCE: mongodb://<mongo_uri>:27017/?replicaSet=<replica_set>
# - TARGET: mongodb://<mongo_uri>:27017/?replicaSet=<replica_set>
# Run the docker container
docker run -it --rm \
--name mongo-repl \
-p 3000:3000 \
--mount type=bind,source=$parent_dir/conf/config.dev.yaml,target=/app/conf.yaml,readonly \
--env-file $parent_dir/conf/.env -d \
sebastienferry/mongo-repl:v0.0.12
