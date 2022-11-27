# Substreams MongoDB Sink

This is a command line tool to quickly sync a substream with a MongoDB database.

### Running It

1) Your substream needs to implement a `map` that has an output type of `proto:substreams.database.v1.DatabaseChanges`.
   By convention, we name the `map` module `db_out`. The [substreams-data-change](https://github.com/streamingfast/substreams-database-change) crate, contains the rust objects.


2) Create a schema for the database changes

   For the moment, the only supported types are:
   - INTEGER
   - DOUBLE
   - BOOLEAN
   - TIMESTAMP
   - NULL
   - DATE
   - STRING (default value for mongodb)

   The schema must be json file and its path will be passed to the sink `run` command.

Here is an example of a schema:
```json
{
  "pair": {
    "created_at" : "timestamp",
    "block_num": "integer"
  }
}
```

> Note: any field which is of type string does not need to be declared in the schema since it will be automatically considered as a string.


3) Run the sink

| Note: to connect to substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|

Usage:

```shell
Runs  extractor code

Usage:
  substreams-sink-mongodb run <dsn> <database_name> <schema> <endpoint> <manifest> <module> [<start>:<stop>] [flags]

Flags:
  -h, --help        help for run
  -k, --insecure    Skip certificate validation on GRPC connection
  -p, --plaintext   Establish GRPC connection in plaintext

Global Flags:
      --delay-before-start duration   [OPERATOR] Amount of time to wait before starting any internal processes, can be used to perform to maintenance on the pod before actually letting it starts
      --metrics-listen-addr string    [OPERATOR] If non-empty, the process will listen on this address for Prometheus metrics request(s) (default "localhost:9102")
      --pprof-listen-addr string      [OPERATOR] If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/) (default "localhost:6060")
```

Example:

```shell
go install ./cmd/substreams-sink-mongodb
substreams-sink-mongodb run \
mongodb://root:root@localhost:27017 \
tokens \
./schema.json \
mainnet.eth.streamingfast.io:443 \
./substreams-v0.0.1.spkg \
db_out
```