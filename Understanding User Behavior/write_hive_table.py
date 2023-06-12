#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_event(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] != 'default':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()


    game_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        #.filter(is_event('raw')
        

    extracted_events = game_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_events.printSchema()
    extracted_events.show()

    extracted_events.registerTempTable("extracted_events")

    spark.sql("""
        create external table game_events
        stored as parquet
        location '/tmp/game_events'
        as
        select * from extracted_events
    """)


if __name__ == "__main__":
    main()