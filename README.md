# kinesis-splitter
Simple tool to split each shards of the given AWS Kinesis stream in two.

##Run

```bash
$ mvn clean install

$ java -jar target/kinesis-splitter-1.0.jar <stream-name> <aws-access-key> <aws-secret-key>
```