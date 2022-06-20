# Substreams mongodb sink

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
1. Deploy a mongodb (docker, Robo 3T or any kind of tool). If deployed with docker, make sure the port is exposed.

2. Run the below command:
```bash
# local deployment of firehose
substreams-mongodb-sink load ./substreams.yaml db_out --endpoint localhost:9000 -p -s 6810706 -t 6810806 
# run remotely against bsc
substreams-mongodb-sink load ./substreams.yaml db_out --endpoint bsc-dev.streamingfast.io -s 6810706 -t 6810806 
```
