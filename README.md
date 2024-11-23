# Mongo Replication

This repository contains a replication tool for MongoDB databases. The aim of this tool
are to keep two MongoDB databases, one source and one target, synchronized.

This tool is as the moment a proof of concept inspired by MongoShake. 

## Features

- Initial idempotent full sync (update if data already exists)
- Always running. The service, once started, keep on synching source and target
- Kubernetes ready using `/status` liveness endpoint
- Configure collections white list or black list (exclusive)
- QPS limiter driven by the read on the source database (not yet configurable)
- Prometheus reporting using `/metrics` endpoint

## Planned

- OPLog replay
- CLI mode to do a oneshot full sync
- Control API to pause/resume and trigger snapshots

## Usage

The tool fetch its configuration from a local `yaml` file. A sample if provided
in the `/conf` directory. The env variable `CONFIG_FILE_PATH` is used to pass the
path to the configuration file at start. See [config](./docs/config.md).

## Contributions

This project is not yet open to contributions. This might change in the future
once the very basic features would have been implemented.