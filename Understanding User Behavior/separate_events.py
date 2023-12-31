#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
#     event['Host'] = "moe"
    event['Cache-Control'] = "no-cache"
    return json.dumps(event)


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_events = munged_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    
    # write all events to file
    extracted_events.show()
    extracted_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/all_events")

    # filter purchase events
    purchases = extracted_events \
        .filter(extracted_events.event_type == 'purchase')
    purchases.show()
    purchases \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/purchases")
    
    # filter upgrade events
    upgrades = extracted_events \
        .filter(extracted_events.event_type == 'upgrade')
    upgrades.show()
    upgrades \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/upgrades") 
    
    # filter sell events
    sales = extracted_events \
        .filter(extracted_events.event_type == 'sell')
    sales.show()
    sales \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/sales")
    
    # filter ditch events
    ditch = extracted_events \
        .filter(extracted_events.event_type == 'ditch')
    ditch.show()
    ditch \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/ditch")


    # filter default events
    default_hits = extracted_events \
        .filter(extracted_events.event_type == 'default')
    default_hits.show()
    default_hits \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/default_hits")


if __name__ == "__main__":
    main()
