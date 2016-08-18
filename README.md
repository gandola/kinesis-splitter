# kinesis-splitter
Simple tool to split a specific shard of the given AWS Kinesis stream in two.

You can also list all the shards of a stream using this tool

##Run

```bash
$ mvn clean install

# To split a specific shard
$ java -jar target/kinesis-splitter-1.0.jar <stream-name> <aws-access-key> <aws-secret-key> <shard_id_to_split>

# To list all the shards of a stream, provide the ID of the shard to split as some random name like DUMMY_SHARD_ID
java -jar target/kinesis-splitter-1.0.jar <stream-name> <aws-access-key> <aws-secret-key> DUMMY_SHARD_ID
```
