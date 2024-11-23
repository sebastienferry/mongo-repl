# Mongo Replication - Configuration

## Configuration source priority

Some configuration values may be set via different means. In that case, the
following general rule is applied:

```
Cmd > Env > File
```

Meaning, env variable overrides file based values and command line arguments overrides env variables.

## Configuration file path

- **Description**: Gives the path to the configuration file
- **Mandatory**: yes
- **Cmd**: `-c <string>`
- **Env**: `CONFIG_FILE_PATH`
- **File**: n/a

## Source database

- **Description**: Gives the source DB MongoDB URI
- **Mandatory**: yes
- **Cmd**: n/a
- **Env**: `SOURCE`
- **File**: `repl.source`

## Target database

- **Description**: Gives the target DB MongoDB URI
- **Mandatory**: yes
- **Cmd**: n/a
- **Env**: `TARGET`
- **File**: `repl.target`

## File based configuration options

Check out the sample provided [here](../conf/config.sample.yaml).