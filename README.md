# Substreams Sink to MongoDB

> Outdated, we are in the process of upgrading this project, right now, it's non-functioning.

### Create a schema for the database changes
For the moment, we only support these types:
- INTEGER
- DOUBLE
- BOOLEAN
- TIMESTAMP
- NULL
- DATE
- STRING (default value for mongodb)

The schema has to be a json file and has to be passed in with the flag `--mongodb-schema` (default value is `./schema.json`).

Here is an example of a schema:
```json
{
  "pair": {
    "timestamp" : "timestamp",
    "block": "integer"
  }
}
```

> Note: any other field which is of type string isn't necessary to be declared

### Running mongodb cli loader
1. Deploy mongodb with docker (you can also use any other tool or method to deploy a local mongodb)
```bash
docker run --rm -d --publish 27017:27017 mongo
```

2. Run the below command:
```bash
# local deployment of firehose
substreams-sink-mongodb load ./substreams.yaml db_out --endpoint localhost:9000 -p -s 6810706 -t 6810806
# run remotely against bsc
sftoken # https://substreams.streamingfast.io/reference-and-specs/authentication
substreams-sink-mongodb load ./substreams.yaml db_out --endpoint bsc-dev.streamingfast.io -k -s 6810706 -t 6810806
```
