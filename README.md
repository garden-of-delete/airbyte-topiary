# Airbyte Tentacle
Airbyte tentacle is an open-source configuration and deployment management tool for [Airbyte](https://github.com/airbytehq/airbyte). **As this tool is early in development, I *highly* recommend reading below before using the tool to avoid irreversible (potentially unexpected) changes to your Airbyte deployment.**

# Releases
No releases yet.

# Setup
1. Clone this repo to your working environment
`git clone github.com/garden-of-delete/airbyte-tentacle`
2. Create a new Python3 virtual environment. For example, to use venv to create a virtual environment in your current working directory:
`python3 -m venv .venv`
Activate the environment with `. .venv/bin/activate`
3. Install all requirements:
`pip3 install requirements.txt`
4. Run Tentacle. `python tentacle.py` will display help and usage.

# Configuration as YAML
Airbyte tentacle allows configuration for an airbyte deployment to be moved to and from yaml files through interaction with the Airbyte API. Provided .yml configuration is first validated, but care should be taken to ensure all the details are correct. Check the `examples/` directory to see some example configurations.

### Sources
Sources require the following:  
`name`: a name given to the source. **should be unique across the whole Airbyte deployment**  
`sourceName`: the name associated with the Airbyte connector. e.g. GitHub, Slack. Used to choose the right connector type when creating a new source.  
`connectionConfiguration`: specific to each source. Check the documentation for that source to get a list. Will include things like:  
    `access_token` / `api_token` / `some_other_secret`  
    `repository`  
    `start_date` (provided as a standard timestamp YYYY-MM-DDThr:mm:ssZ)  

Optionally, a `sourceId` (uuid) can be provided to bypass using `name` to check if the source already exists in the Airbyte deployment during a `sync` operation.

### Destinations
Same as Sources, but will probably have more destination specific details in the `connectionConfiguration` section. For example, the BigQuery destination requires something like:
```
    big_query_client_buffer_size_mb: 15
      credentials_json: '**********'
      dataset_id: somedataset
      dataset_location: US
      project_id: some-project
```
Optionally, a `destinationId` (uuid) can be provided to bypass using `name` to check if the source already exists in the Airbyte deployment during a `sync` operation.

### Connections
Connections require the following:  
`sourceName` or `sourceId`: used to identify the source. Id will be tried first, then name.  
`destinationName` or `destinationId`: used to identify the destination. Id will be tried first, then name.  
`connectionName` or `connectionId`: used to provide a name for a new connection (not visible in Airbyte's GUI), or to target an existing connection for changes  
`prefix`: prefixes the tables produced by the connection. For example `github_superset_`  
`namespaceDefinition`: tells the connection to use the namespace configuration (schema / dataset information, other details) of the source, destination, or custom. I personally leave the namespace configuration up to the destination (`destination`).  
`schedule`:  
    `units`: number of units of time as an integer  
    `timeUnit`: units of time used (`hours`, `days`, etc)  
`status`: active or inactive. Note: an "active" connector with a schedule will start a sync attempt in Airbyte immediately upon creation.

Optionally, a `syncCatalog` can also be specified. This monstrosity is specific to each source and contains the configuration for each of the streams in the connection. Since the `syncCatalog` as expected by the Airbyte API is not particularly human readable, tentacle provides some options here:
- If a `syncCatalog` is not provided, tentacle will retrieve the default sync catalog from the source and use that. Note, the default syncCatalog has all available streams selected with the default sync mode (usually "Full Refresh - Overwrite/Append")
- To modify the `syncCatalog` for an existing connection, I would recommend first syncing the connection to yaml before making changes and applying back to Airbyte. Read the "**The Sync Workflow**" section below to see how to do this.

A connection connecting a GitHub source to a BigQuery destination might look something like this (no SyncCatalog provided, so defaults will be used):
```
 - sourceName: apache/superset
    destinationName: community-data-bq
    prefix: 'github_superset_'
    namespaceDefinition: destination
    schedule:
        units: 24
        timeUnit: hours
    status: 'inactive'
```

# Workflows
Airbyte tentacle supports a number of workflows designed to make managing Airbyte deployments at scale easier. These are:
- **sync**: applies configuration provided as yml to an Airbyte deployment, OR retrieves the configuration of an Airbyte deployment and writes it to .yml
- **wipe**: deletes the specified connectors (sources, destinations) and associated connections / configuration
- **validate**: validates all sources and destinations

## The Sync Workflow
All the sync workflows described below are accessed through the **sync** master mode like so:

`python tentacle.py sync ...`

In all cases, a configuration origin, which follows the `sync` command, and a `--target` are required.

Tentacle will use the .yaml or .yml file extensions following the `source` and `--target` arguments to choose the right sync workflow.

During setup, Airbyte creates a default workspace called 'default'. Tentacle allows the user to specify an alternative existing workspace by name using the optional `--workspace` argument, followed by the name of the workspace.

### Sync yaml to Airbyte

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

There are a number of additional optional parameters that modify how a sync operation is carried out:
- `--wipe` removes all sources, destinations, and connectors **before** applying config.yml
- `--backup` followed by a filename. Dumps the full configuration of airbyte to the specified file **before** applying `--wipe` and config.yml
- `--validate` validates the sources, destinations, and connections on the destination Airbyte deployment **after** applying changes.

Used together, a realistic invocation of tentacle might look something like:
`python tentacle.py sync config.yml --target http://123.456.789.0:8081 --secrets secrets.yml --all --validate --backup backup_config.yml`

### Modifying existing sources and destinations
If the yaml file specifies connectors with valid `sourceId`, `destinationId`, or `connectionId` matching matching Airbyte deployment, or failing that, valid `name`s, then tentacle will attempt to modify the existing source/destination/connection instead of creating a new one. 

### Sync deployment to yaml
An existing Airbyte deployment can be written to a .yaml by following the `--target` argument with a filename having the .yaml or .yml extension. For example:

`python tentacle.py sync http://123.456.789.0:8081 --target my_deployment.yml`

will write the configuration of all sources, destinations, and connections to `my_deployment.yml`.

Note in this case, no `--secrets` file is specified, since it has no meaning in this workflow. Secrets can't be extracted from the Airbyte API.

## Wipe a deployment
The `wipe` mode deletes sources, destinations, connections or any combination in an existing Airbyte deployment.

`python tentacle.py wipe http://123.456.789.0:8081 --all`

As with `sync`ing a .yaml file to a deployment, these optional arguments can be used in combination to define what to apply the wipe operation to:
- `--sources`
- `--destinations`
- `--connections`
- `--all` (same as `--sources --destinations --connections`)

**Note**: the `--wipe` argument when used in the `sync` workflow will wipe **ALL** sources/destinations/connections, not just those specified.

## Validate a deployment
The `validate` mode validates sources, destinations, connections or any combination in an existing Airbyte deployment.

`python tentacle.py validate http://123.456.789.0:8081 --all`

As with `wipe` mode, these optional arguments can be used in combination to define what to apply the `validate` operation to:
- `--sources`
- `--destinations`
- `--connections`
- `--all` (same as `--sources --destinations --connections`)

# Contributing
This is a small project i've been building in my free time, so there isn't much structure needed around contributing (for now). Check the issue list, open an issue for your change if needed, fork the project, modify it, then open a PR :)

# Acknowledgements
Thanks to Abhi and the Airbyte team for being responsive to questions and feedback during the development process. Also big thanks to the team at Preset.io for supporting the concept and my use of Airbyte while employed there.

# License
Copyright 2021 Robert Stolz

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.