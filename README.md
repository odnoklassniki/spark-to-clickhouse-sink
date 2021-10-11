# spark-to-clickhouse-sink

A thick-write-only-client for writing across several ClickHouse MergeTree tables located in different shards.  
It a good alternative to writing via Clickhouse Distributed Engine which has been proven to be a bad idea for several reasons. 

The core functionality is the [writer](https://github.com/odnoklassniki/spark-to-clickhouse-sink/blob/main/src/main/java/odkl/spark/to/clickhouse/SparkToClickhouseWriter.java). It works on top of Apache Spark and takes DataFrame as an input. 

## Streaming and Exactly-Once Samantic
The writer can also write data w/o duplicates from repeatable source. For example it can be very useful in achieving
EOS semantic when Kafka-Clickhouse sink is needed.
It is a good alternative to ClickHouse Kafka Engine. 
For make it work the writer needs the following.
- A sorting key will be used to sort rows inside a partition. It could be any columns which lead to a stable sorting inside a partition.
- The mechanism relies on Clickhouse deduplication for same data blocks. See: https://clickhouse.tech/docs/en/operations/settings/settings/#settings-insert-deduplicate.

Here is a pseudo-code how it could be used to consume data from Kafka and insert into ClickHouse written in Spark Structured Streaming (ver. 2.4+)
```
val streamDF = spark.readStream()
                	  .format("kafka")
                    .option(<kafka brokers and topics>)
                    .load();

val writer = new SparkToClickHouseWriter(<my_conf>)

streamDF.forEachBatch(df -> writer.write(df))
```

## Refs
There is [a talk](https://smartdataconf.ru/en/talks/insert-into-clickhouse-and-not-die/) about problems with ClickHouse Distributed and Kafka engines and reasons which forced us to implement this util library. 
