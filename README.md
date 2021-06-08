# Airbyte Tentacle
Purpose

## Releases

## Setup
1. Clone this repo to your working environment
`git clone github.com/garden-of-delete/airbyte-tentacle`
2. Create a new Python3 virtual environment. For example, to use venv to create a virtual environment in your current working directory:
`python3 -m venv .venv`
Activate the environment with `. .venv/bin/activate`
3. Install all requirements:
`pip3 install requirements.txt`
4. Run Tentacle. `python tentacle.py` will display help and usage.

## Configuration as YAML

## Workflows
Airbyte tentacle supports a number of workflows designed to make managing Airbyte deployments at scale easier. Tentacle uses the .yml or .yaml

### The Sync Workflows
All the sync workflows described below are accessed through the **sync** master mode.

In all cases, a configuration origin, which follows the `sync` command, and a `--target` are required.

These optional arguments can be used in combination to define what to apply the sync operation to:
- `--sources`
- `--destinations`
- `--connections`
- `--all` (same as `--sources --destinations --connections`)

Note if none of these arguments are given, no changes will be made to the `--destination`.

**Sync yaml to deployment**
The yaml to deployment workflow takes a .yaml file as the origin and applies the configuration contained within to a destination Airbyte deployment.

Basic usage could be something like:
`python tentacle.py sync config.yml --target http://123.456.789.0:8081 --all`

Almost all sources and destinations will have associated secrets. Tentacle ignores any secrets specified in the source config.yml. Secrets are specified separately using the `--secrets` argument, followed by a .yaml file. For example:

`python tentacle.py sync config.yml --target http://123.456.789.0:8081 --secrets secrets.yml --all`

There are a number of additional optional parameters that modify how a sync operation is carried out when syncing yaml to deployment.
- `--wipe` removes all sources, destinations, and connectors **before** applying config.yml
- `--validate` validates the sources, destinations, and connections on the destination Airbyte deployment **after** applying changes.
- `--dump` dumps the full configuration of the destination deployment to a configuration
**before** applying the sync operation.

### Sync Deployment to yaml
An existing Airby

### Sync Deployment to deployment

### Wipe a deployment

### Validate a deployment

### Update a deployment

## Design

## Contributing

## Acknowledgements

## License