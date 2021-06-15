# Airbyte Tentacle
Airbyte tentacle is a work-in-progress configuration and deployment management tool for [Airbyte](https://github.com/airbytehq/airbyte).

## Releases
No releases yet.

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
Airbyte tentacle allows configuration for an airbyte deployment to be moved to and from yaml files through interaction with the Airbyte API.

There will be a guide here that explains the config spec with examples.

## Workflows
Airbyte tentacle supports a number of workflows designed to make managing Airbyte deployments at scale easier. Tentacle uses the .yml or .yaml

### The Sync Workflows
All the sync workflows described below are accessed through the **sync** master mode like so:

`python tentacle.py sync`

In all cases, a configuration origin, which follows the `sync` command, and a `--target` are required.

Tentacle will use the .yaml or .yml file extensions following the `source` and `--target` arguments to choose the right sync workflow.

During setup, Airbyte creates a default workspace called 'default'. Tentacle allows the user to specify an alternative existing workspace by name using the optional `--workspace` argument, followed by the name of the workspace.

**Sync yaml to deployment**

The yaml to deployment workflow takes a .yaml file as the origin and applies the configuration contained within to a destination Airbyte deployment.

These optional arguments can be used in combination to define what to apply the sync operation to:
- `--sources`
- `--destinations`
- `--connections`
- `--all` (same as `--sources --destinations --connections`)

**Note:** if none of the four optional arguments above are given, no changes will be made to the `--target`.

Basic usage could be something like:

`python tentacle.py sync config.yml --target http://123.456.789.0:8081 --all`

Almost all sources and destinations will have associated secrets. Tentacle ignores any secrets specified in the source config.yml. Secrets are specified separately using the `--secrets` argument, followed by a .yaml file. For example:

`python tentacle.py sync config.yml --target http://123.456.789.0:8081 --secrets secrets.yml --all`

There are a number of additional optional parameters that modify how a sync operation is carried out when syncing yaml to deployment.
- `--wipe` removes all sources, destinations, and connectors **before** applying config.yml
- `--validate` validates the sources, destinations, and connections on the destination Airbyte deployment **after** applying changes.
- `--dump` dumps the full configuration of the destination deployment to a configuration
**before** applying the sync operation.

**Sync deployment to yaml**
An existing Airbyte deployment can be written to a .yaml by following the `--target` argument with a filename having the .yaml or .yml extension. For example:

`python tentacle.py sync http://123.456.789.0:8081 --target my_deployment.yml`

Will write the configuration of all sources, destinations, and connections to `my_deployment.yml`.

Note in this case, no `--secrets` file is specified, since it has no meaning in this workflow. Secrets can't be extracted from the Airbyte API.

### Wipe a deployment

### Validate a deployment

## Design

## Contributing

## Acknowledgements

## License
Copyright 2021 Robert Stolz

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.