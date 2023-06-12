# Project 2: Tracking User Activity

Preparing the infrastructure to land the data in the form and structure it needs to be to be queried incorporating docker containers, Kafka, Spark, and Hadoop. Ed Tech Firm specializes are in administering a number of tests, and a common issue the data scientists may enconter is data latency among data warehouses. A number of exam questions are dumped into a service layer before the actual test. The real-time data that are accumulated as exam takers submit their answers are stored in monitoring systems. In this project I am establishing multiple pipelines connecting several of our data warehouses to streamline the exam data.

## Project Overview

Through 3 different activities, I will spin up the containers and prepae the infrastructure to land the data in the form and structure it needs to be to be queried. Specifically:
- Published and consumed messages with kafka
- Used spark to transform the messages
- Landed the messages in hdfs.

In the developed pipeline schema, Kafka serves as a central warehouse from which different service centers consume or produce data. Kafka is also used to overcome the latency issue. Spark offers variou different tools and is used for analyzing the data. All data analyized and engineered in Spark was later stored in Hadoop HDFS.

## Introduction

To process this data, I log into my Google Cloud Platform, create a folder with a yml file with kafka, zookeeper, spark, hadoop, and the mids containers. Then I download the data, and spin up a docker composer with kafka, zookeeper, spark, hadoop, and the mids containers. I then publish the messages and consume them with kafka, consume and print data using spark, transform and land messages into hdfs, and finally exit both spark and kafka, and spin down my docker-composer.

## Data Preparation

After logging into my GCP, I created a new directory, and placed a new .yml file as shown below. The .yml file includes zookeeper, kafka, hadoop, spark, and the mids images, respectively.


```
---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 32181
      ZOOKEEPER_TICK_TIME: 2000
    expose:
      - "2181"
      - "2888"
      - "32181"
      - "3888"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    expose:
      - "9092"
      - "29092"

  cloudera:
    image: midsw205/cdh-minimal:latest
    expose:
      - "8020" # nn
      - "50070" # nn http
      - "8888" # hue
    #ports:
    #- "8888:8888"

  spark:
    image: midsw205/spark-python:0.0.5
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    command: bash
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera

  mids:
    image: midsw205/base:latest
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
```

Then, I downloaded the json file to my w205/project-2-<your folder> folder using the terminal with the following command.

```
curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/ME6hjp`
```
#### Spin up the docker-compose

Then I spun up the docker-composer in the detached mode using -d, and started the kafka logs. Then, I switched to a different terminal window to continue.

```
docker-compose up -d
docker-compose logs -f kafka
```

Pressed `ctrl+c` to after logs command. Once my container was spun up and running, I checked if all the services are running correctly.

```
docker-compose ps
```

```
project-2-<your folder>_cloudera_1    cdh_startup_script.sh       Up      11000/tcp, 11443/tcp, 19888/tcp, 50070/tcp, 
project-2-<your folder>_kafka_1       /etc/confluent/docker/run   Up      29092/tcp, 9092/tcp                      project-2-<your folder>_mids_1        /bin/bash                   Up      8888/tcp                                 project-2-<your folder>_spark_1       docker-entrypoint.sh bash   Up                                                project-2-<your folder>_zookeeper_1   /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 32181/tcp, 3888/tcp                             
```
All services are up and good to go. I ran the following code to view the hadoop hdfs file system at /tmp/ folder to make sure what I write isn't there already:

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

So far there are only two items in the /tmp/ folder. The output of the command is:

```
Found 2 items
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-02-23 06:57 /tmp/hive
```
#### Create a topic in Kafka

I then created a topic, "exams" in the kafka container using the following code. This code also shows that I created 1 partition, had a replication factor of 1, and am using zookeeper to manage the process. I used the kafka-topics utility to do this.

```
docker-compose exec kafka \
  kafka-topics \
    --create \
    --topic exams \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --zookeeper zookeeper:32181
```

The expected output was the following topic is created. I call my kafka topic "exams" as it includes the info of each exam like examinee's name, name of exam, and the assesment of each question in the exam. 

```
Created topic "exams"
```

Then, I used the kafka-topics utility to check the topic that I just created in the kafka container as shown below:

```
docker-compose exec kafka \
  kafka-topics \
  --describe \
  --topic exams \
  --zookeeper zookeeper:32181
```

My output was:

```
Topic: players  PartitionCount: 1       ReplicationFactor: 1    Configs: 
        Topic: exams  Partition: 0    Leader: 1       Replicas: 1     Isr: 1
```
#### Produce messages in Kafka

As shown, the output matches my configuration, so looks like everything is running as it should be. Then, I ran the docker-compose exec command kafkacat to produce messages to the "exams" topic. What this command does is to read in the json file previously downloaded and used the kafkacat utility to publish the messages to the "exams" topic in kafka.

```
docker-compose exec mids \
   bash -c "cat /w205/project-2-<your folder>/assessment-attempts-20180128-121051-nested.json | jq '.[]' \
     -c | kafkacat -P -b kafka:29092 \
     -t exams"
```

there was no output, as expected.

#### Consume messages in Kafka

I then consumed 10 messages from the assessments topic. I did this using the kafka-console-consumer utility. I read them in from the beginning. However, it shows all messages.
```
docker-compose exec kafka \
  kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic exams \
    --from-beginning 
    --max-messages 10
```

Then, I determined the word count using the following code. The output should match the number of assessments that I will check later using Spark:

```
docker-compose exec mids \
  bash -c "kafkacat -C -b kafka:29092 \
    -t exams -o beginning -e" | wc -l
```

The word count is 3281.

#### Run Spark

Then, I run the following command in the terminal windows to run my Spark container. With Apache Spark container, we can directly pipe our Kafka messages into Spark and analyze the data in Spark.
 
```
docker-compose exec spark pyspark
```

The first thing I do is to consume messages from the Kafka topic "exams", and create a Spark dataframe called "raw_exams".

```
raw_exams = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe","exams") \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load() 
```

Then, I used the following command to cache the data structure. This way, it cuts down the number of warning messages that aren't useful. It also help with holding a persistent handle to the topic.

```
raw_exams.cache()
```

The output is:

```
DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
```

I then printed the schema using the following code, and seeing the following output:

```
raw_exams.printSchema()
```
My output is:

```
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

I used the following code to cast the values from binary to string.

```
exams_str = raw_exams.select(raw_exams.value.cast('string'))
```
Again check the schema to see how casting to string works.

```
exams_str.printSchema()
```
The output, as shown below, indicates that it is successfullt converted to a string:

```
root
 |-- value: string (nullable = true)
```

Then, I wrote the exams_str data frame into a parquet file in the hadoop hdfs. This is a binary format and immutable.

```
exams_str.write.parquet("/tmp/exams_str")
```

I then switched back to another docker-compose command line window and used the following command to view the directory /tmp/ in hadoop hdfs. 

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
The Output was shown below. I noticed that a folder /exams_str is added to my /tmp/ as a result of running `.write.parquet` function. Below we can see /tmp/exams_str added as a directory and not a file to hadoop hdfs.  

```
drwxr-xr-x   - root   supergroup          0 2021-02-26 07:34 /tmp/exams_str
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-02-26 07:00 /tmp/hive
```
Now I check the content inside /tmp/exams_str/

```
docker-compose exec cloudera hadoop fs -ls /tmp/exams_str/
```

The output is shown below. It shows two items added.
```
Found 2 items
-rw-r--r--   1 root supergroup          0 2021-03-02 05:44 /tmp/exams_str/_SUCCESS
-rw-r--r--   1 root supergroup    2513397 2021-03-02 05:44 /tmp/exams_str/part-00000-bbbba3a2-7aa6-4174-a19f-9d28d7f3c303-c000.snappy.parquet
```

Then, I went back to my spark container and printed the top 20 rows of the players data frame usinh the .show() command.

```
exams_str.show()
```
The output is:

```
+--------------------+
|               value|
+--------------------+
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
+--------------------+
only showing top 20 rows
```

That is not clean. It only shows the keen_timestamp column. So, let's extract more fields and promote data columns to be real dataframe columns.

#### JSON Analysis and Unrolling

I imported the sys package and then set a standard output that writes data to utf-8 instead of unicode. This is generally a good practice in dealing with unicode data.

```
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
```

Because the output was difficult to read and decipher, I imported and used the json package to print out some of the data. Below is the code with the following print output.

```
import json
print(json.loads(exams_str.select('value').take(1)[0].value).keys()) 
```

Output:
```
dict_keys(['keen_timestamp', 'max_attempts', 'started_at', 'base_exam_id', 'user_exam_id', 'sequences', 'keen_created_at', 'certification', 'keen_id', 'exam_name'])
```

Then I transform the exams_str into dataframe (df) using Lambda function and json.loads and then show it:
```
exams_str.rdd.map(lambda x: json.loads(x.value)).toDF().show()
```

Looks like it extracts some columns. Now let's create a new dataframe to hold the data in json, and then print off the first 20 rows. Below is the code with the following output (and warning message). Extract some more fields using a Lambda function and convert them into DataFrame and stored it in `extracted_exams_str`:

```
extracted_exams_str = exams_str.rdd.map(lambda x: json.loads(x.value)).toDF()
extracted_exams_str.show()
```

Below I am only showing a few records of the 20 rows the output gives:
```
--------------------+--------------------+
|        base_exam_id|certification|           exam_name|   keen_created_at|             keen_id|    keen_timestamp|max_attempts|           sequences|          started_at|        user_exam_id|
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717442.735266|5a6745820eb8ab000...| 1516717442.735266|         1.0|Map(questions -> ...|2018-01-23T14:23:...|6d4089e4-bde5-4a2...|
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717377.639827|5a674541ab6b0a000...| 1516717377.639827|         1.0|Map(questions -> ...|2018-01-23T14:21:...|2fec1534-b41f-441...|
|4beeac16-bb83-4d5...|        false|The Principles of...| 1516738973.653394|5a67999d3ed3e3000...| 1516738973.653394|         1.0|Map(questions -> ...|2018-01-23T20:22:...|8edbc8a8-4d26-429...|
|4beeac16-bb83-4d5...|        false|The Principles of...|1516738921.1137421|5a6799694fc7c7000...|1516738921.1137421|         1.0|Map(questions -> ...|2018-01-23T20:21:...|c0ee680e-8892-4e6...|
|6442707e-7488-11e...|        false|Introduction to B...| 1516737000.212122|5a6791e824fccd000...| 1516737000.212122|         1.0|Map(questions -> ...|2018-01-23T19:48:...|e4525b79-7904-405...|
|8b4488de-43a5-4ff...|        false|        Learning Git| 1516740790.309757|5a67a0b6852c2a000...| 1516740790.309757|         1.0|Map(questions -> ...|2018-01-23T20:51:...|3186dafa-7acf-47e...|
|e1f07fac-5566-4fd...|        false|Git Fundamentals ...|1516746279.3801291|5a67b627cc80e6000...|1516746279.3801291|         1.0|Map(questions -> ...|2018-01-23T22:24:...|48d88326-36a3-4cb...|
```

As shown above the json.load function was able to extract one layer of the nested JSON data. But the problem is more severe than that as for example, the column 'sequences' itself is also nested. We see it is almost useless because of the nesting. Let's take a look at the schema of `extracted_exams_str`:

```
extracted_exams_str.printSchema()
```
The output is shown below. As shown it still has nested layers of data. It clearly shows that 'sequences' column is nested and inside 'sequences', there are also nested columns that need to be flattened.
```
root
 |-- base_exam_id: string (nullable = true)
 |-- certification: string (nullable = true)
 |-- exam_name: string (nullable = true)
 |-- keen_created_at: string (nullable = true)
 |-- keen_id: string (nullable = true)
 |-- keen_timestamp: string (nullable = true)
 |-- max_attempts: string (nullable = true)
 |-- sequences: map (nullable = true)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = true)
 |    |    |-- element: map (containsNull = true)
 |    |    |    |-- key: string
 |    |    |    |-- value: boolean (valueContainsNull = true)
 |-- started_at: string (nullable = true)
 |-- user_exam_id: string (nullable = true)
```

Also, the schema shows Null for the nested data which have both 'string' and integer types. So transforming the raw data into a dataframe as shown below gave me the right schema.

```
extracted_exams_string = spark.read.json(exams_str.rdd.map(lambda x: x.value))
extracted_exams_string.printSchema()
```

```
root
 |-- base_exam_id: string (nullable = true)
 |-- certification: string (nullable = true)
 |-- exam_name: string (nullable = true)
 |-- keen_created_at: string (nullable = true)
 |-- keen_id: string (nullable = true)
 |-- keen_timestamp: string (nullable = true)
 |-- max_attempts: string (nullable = true)
 |-- sequences: struct (nullable = true)
 |    |-- attempt: long (nullable = true)
 |    |-- counts: struct (nullable = true)
 |    |    |-- all_correct: boolean (nullable = true)
 |    |    |-- correct: long (nullable = true)
 |    |    |-- incomplete: long (nullable = true)
 |    |    |-- incorrect: long (nullable = true)
 |    |    |-- submitted: long (nullable = true)
 |    |    |-- total: long (nullable = true)
 |    |    |-- unanswered: long (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- questions: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- options: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- at: string (nullable = true)
 |    |    |    |    |    |-- checked: boolean (nullable = true)
 |    |    |    |    |    |-- correct: boolean (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- submitted: long (nullable = true)
 |    |    |    |-- user_correct: boolean (nullable = true)
 |    |    |    |-- user_incomplete: boolean (nullable = true)
 |    |    |    |-- user_result: string (nullable = true)
 |    |    |    |-- user_submitted: boolean (nullable = true)
 |-- started_at: string (nullable = true)
 |-- user_exam_id: string (nullable = true)
```

Now I am ready to store the dataframe with the right structure in the hdfs using utf-8 and saving in HDFS in parquet format

```
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
extracted_exams_string.write.parquet('/tmp/extracted_exams_string')
```
Let's check the /tmp/ content using another terminal to see what we just stored.

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
The output shows the dataframe is stored.

```
Found 4 items
drwxr-xr-x   - root   supergroup          0 2021-03-02 05:44 /tmp/exams_str
drwxr-xr-x   - root   supergroup          0 2021-03-02 06:08 /tmp/extracted_exams_string
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-03-02 05:34 /tmp/hive
```

Here for the sake of illustration, I have commands to store and retrieve parquet file from Hadoop.
- How to store data and read back in parquet format in HDFS. 

```
>>> DF_name.write.parquet('/tmp/myfile')
>>> my_file = sqlContext.read.parquet('/tmp/myfile')
```

#### Use SparkSQL to Query

Now let's use SparkSQL to deal with multilevel nested JSON. First, I create a Spark "TempTable" using the following code:

```
extracted_exams_string.registerTempTable('exams')
```

SparkSQL will let us easily pick and choose the fields we want to promote to columns. Then we can create DataFrames from queries. Below I am running a number of queries to answer some business questions.

The following query shows the top ten exam names 

```
spark.sql("select exam_name from exams limit 10").show()
```

```
+--------------------+
|           exam_name|
+--------------------+
|Normal Forms and ...|
|Normal Forms and ...|
|The Principles of...|
|The Principles of...| 
|Introduction to B...|
|        Learning Git|
|Git Fundamentals ...|
|Introduction to P...|
|Intermediate Pyth...|
|Introduction to P...|
+--------------------+
```
Question 1. How many assesstments are in the dataset?

The following query shows how many exams are in the dataset. I am counting the number of `exam_name`. It looks like there is a total of 3280 exams as shown below. 

```
spark.sql("select count(exam_name) from exams").show()
```

```
+----------------+
|count(exam_name)|
+----------------+
|            3280|
+----------------+
```

Question 2. How many unique exam_names are there?

The following query shows the number of unique exam names. Looks like there are 103 unique exam_names.

```
spark.sql("select count(distinct(exam_name)) from exams").show()
```
```
+-------------------------+                                                     
|count(DISTINCT exam_name)|
+-------------------------+
|                      103|
+-------------------------+
```

Question 3. How many people took each exam?

The following query the number examinees for each exam. It is also sorted which shows what courses are mostly taken. Looks like Learning Git is taken by 349 people which is the highest number of people taking an exam.

```
spark.sql("SELECT exam_name, COUNT(exam_name) as exam_count FROM exams GROUP BY exam_name ORDER BY exam_count DESC").show()
```
```
+--------------------+----------+                                               
|           exam_name|exam_count|
+--------------------+----------+
|        Learning Git|       394|
|Introduction to P...|       162|
|Introduction to J...|       158|
|Intermediate Pyth...|       158|
|Learning to Progr...|       128|
|Introduction to M...|       119|
|Software Architec...|       109|
|Beginning C# Prog...|        95|
|    Learning Eclipse|        85|
|Learning Apache M...|        80|
|Beginning Program...|        79|
|       Mastering Git|        77|
|Introduction to B...|        75|
|Advanced Machine ...|        67|
|Learning Linux Sy...|        59|
|JavaScript: The G...|        58|
|        Learning SQL|        57|
|Practical Java Pr...|        53|
|    HTML5 The Basics|        52|
|   Python Epiphanies|        51|
+--------------------+----------+
only showing top 20 rows
```

Question 4. How many people took Learning Git?

As shown below 349 people took Learnin Git. 

```
spark.sql("SELECT exam_name, COUNT(exam_name) as exam_count FROM exams where exam_name='Learning Git' group by exam_name ").show()
```
```
+------------+----------+                                                       
|   exam_name|exam_count|
+------------+----------+
|Learning Git|       394|
+------------+----------+
```

Question 5. What is the least common course taken? And the most common?

Most common course taken:  

```
spark.sql("SELECT exam_name, exam_count FROM (SELECT exam_name, COUNT(exam_name) as exam_count FROM exams GROUP BY exam_name) aa where aa.exam_count = (SELECT max(exam_count) FROM (SELECT exam_name, COUNT(exam_name) as exam_count FROM exams GROUP BY exam_name)) ").show()
```
```
+------------+----------+                                                       
|   exam_name|exam_count|
+------------+----------+
|Learning Git|       394|
+------------+----------+
```

Least common course taken:

```
spark.sql("SELECT exam_name, exam_count FROM (SELECT exam_name, COUNT(exam_name) as exam_count FROM exams GROUP BY exam_name) aa where aa.exam_count = (SELECT min(exam_count) FROM (SELECT exam_name, COUNT(exam_name) as exam_count FROM exams GROUP BY exam_name)) ").show()
```
```
+--------------------+----------+                                               
|           exam_name|exam_count|
+--------------------+----------+
|Learning to Visua...|         1|
|Nulls, Three-valu...|         1|
|Native Web Apps f...|         1|
|Operating Red Hat...|         1|
+--------------------+----------+
```

Question 6. Add any query(ies) you think will help the data science team.

The following query helps data scientist team to find how each user_exam_id has preformed on each exam and each question of the exam. This table provides the most insightful information from the dataset at exam level and user level.From this table, data scientists can extract useful information. E. g. one can calculate the percentage of exam takers who answered all questions correctly. Also whatever query that we created so far, one can create from the following table. I will save this table in hdfs and will call it `some_exam_info`.

```
spark.sql("SELECT user_exam_id, exam_name, sequences.counts.all_correct, sequences.counts.correct, sequences.counts.incomplete, sequences.counts.incorrect, sequences.counts.submitted, sequences.counts.total, sequences.counts.unanswered FROM exams").show()
```

```
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
|        user_exam_id|           exam_name|all_correct|correct|incomplete|incorrect|submitted|total|unanswered|
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
|6d4089e4-bde5-4a2...|Normal Forms and ...|      false|      2|         1|        1|        4|    4|         0|
|2fec1534-b41f-441...|Normal Forms and ...|      false|      1|         2|        1|        4|    4|         0|
|8edbc8a8-4d26-429...|The Principles of...|      false|      3|         0|        1|        4|    4|         0|
|c0ee680e-8892-4e6...|The Principles of...|      false|      2|         2|        0|        4|    4|         0|
|e4525b79-7904-405...|Introduction to B...|      false|      3|         0|        1|        4|    4|         0|
|3186dafa-7acf-47e...|        Learning Git|       true|      5|         0|        0|        5|    5|         0|
|48d88326-36a3-4cb...|Git Fundamentals ...|       true|      1|         0|        0|        1|    1|         0|
|bb152d6b-cada-41e...|Introduction to P...|       true|      5|         0|        0|        5|    5|         0|
|70073d6f-ced5-4d0...|Intermediate Pyth...|       true|      4|         0|        0|        4|    4|         0|
|9eb6d4d6-fd1f-4f3...|Introduction to P...|      false|      0|         1|        0|        1|    5|         4|
|093f1337-7090-457...|A Practical Intro...|      false|      3|         1|        0|        4|    4|         0|
|0f576abb-958a-4c0...|Git Fundamentals ...|       true|      1|         0|        0|        1|    1|         0|
|0c18f48c-0018-450...|Introduction to M...|      false|      4|         1|        1|        6|    6|         0|
|b38ac9d8-eef9-495...|   Python Epiphanies|      false|      4|         0|        2|        6|    6|         0|
|bbc9865f-88ef-42e...|Introduction to P...|      false|      4|         1|        0|        5|    5|         0|
|8a0266df-02d7-44e...|Python Data Struc...|      false|      3|         0|        1|        4|    4|         0|
|95d4edb1-533f-445...|Python Data Struc...|      false|      3|         0|        0|        3|    4|         1|
|f9bc1eff-7e54-42a...|Working with Algo...|       true|      4|         0|        0|        4|    4|         0|
|dc4b35a7-399a-4bd...|Learning iPython ...|      false|      2|         0|        0|        2|    4|         2|
|d0f8249a-597e-4e1...|   Python Epiphanies|       true|      6|         0|        0|        6|    6|         0|
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
only showing top 20 rows
```

Below is a query to go over the options in each questions. Please note that options are arrays if we look at the schema so one would need to use `explode()` function to extract the values.

```
spark.sql("SELECT count(*) as question_option from exams LATERAL VIEW explode(sequences.questions['options']) as options where options['id'] is not null").show()
```
```
+---------------+
|question_option|
+---------------+
|          14717|
+---------------+
```
Question 7. Add any query(ies) you think will help the data science team.

The following table helps the data scientists to figure out how a exam taker did with a particular exam question for an exam. This table provides question level assessment of the exams. For example, one can calculate overall how the users performed om the first question of the Learning Git course. I am going to call this table `some_question_info` and store it in hdfs.   

```
spark.sql("SELECT user_exam_id, exam_name, sequences.questions[0].user_result as question_0, sequences.questions[1].user_result as question_1, sequences.questions[2].user_result as question_2, sequences.questions[3].user_result as question_3, sequences.questions[4].user_result as question_4, sequences.questions[5].user_result as question_5  FROM exams").show()
```

```
+--------------------+--------------------+-----------+-----------+----------+-----------+-----------+----------+
|        user_exam_id|           exam_name| question_0| question_1|question_2| question_3| question_4|question_5|
+--------------------+--------------------+-----------+-----------+----------+-----------+-----------+----------+
|6d4089e4-bde5-4a2...|Normal Forms and ...|missed_some|  incorrect|   correct|    correct|       null|      null|
|2fec1534-b41f-441...|Normal Forms and ...|    correct|missed_some| incorrect|missed_some|       null|      null|
|8edbc8a8-4d26-429...|The Principles of...|  incorrect|    correct|   correct|    correct|       null|      null|
|c0ee680e-8892-4e6...|The Principles of...|    correct|missed_some|   correct|missed_some|       null|      null|
|e4525b79-7904-405...|Introduction to B...|  incorrect|    correct|   correct|    correct|       null|      null|
|3186dafa-7acf-47e...|        Learning Git|    correct|    correct|   correct|    correct|    correct|      null|
|48d88326-36a3-4cb...|Git Fundamentals ...|    correct|       null|      null|       null|       null|      null|
|bb152d6b-cada-41e...|Introduction to P...|    correct|    correct|   correct|    correct|    correct|      null|
|70073d6f-ced5-4d0...|Intermediate Pyth...|    correct|    correct|   correct|    correct|       null|      null|
|9eb6d4d6-fd1f-4f3...|Introduction to P...| unanswered|missed_some|unanswered| unanswered| unanswered|      null|
|093f1337-7090-457...|A Practical Intro...|missed_some|    correct|   correct|    correct|       null|      null|
|0f576abb-958a-4c0...|Git Fundamentals ...|    correct|       null|      null|       null|       null|      null|
|0c18f48c-0018-450...|Introduction to M...|    correct|    correct|   correct|missed_some|    correct| incorrect|
|b38ac9d8-eef9-495...|   Python Epiphanies|    correct|    correct| incorrect|  incorrect|    correct|   correct|
|bbc9865f-88ef-42e...|Introduction to P...|    correct|    correct|   correct|    correct|missed_some|      null|
|8a0266df-02d7-44e...|Python Data Struc...|    correct|  incorrect|   correct|    correct|       null|      null|
|95d4edb1-533f-445...|Python Data Struc...|    correct| unanswered|   correct|    correct|       null|      null|
|f9bc1eff-7e54-42a...|Working with Algo...|    correct|    correct|   correct|    correct|       null|      null|
|dc4b35a7-399a-4bd...|Learning iPython ...| unanswered|    correct|unanswered|    correct|       null|      null|
|d0f8249a-597e-4e1...|   Python Epiphanies|    correct|    correct|   correct|    correct|    correct|   correct|
+--------------------+--------------------+-----------+-----------+----------+-----------+-----------+----------+
only showing top 20 rows
```

#### Storing and Retrieving Some Queries/Tables in HDFS

Below I am going to save some of those tables that I queried in HDFS. I create a temporary SQL table in order to query. Below I am grabbing some tables and saving them as dataFrames called `some_exam_info` and `some_question_info`. 

```
some_exam_info = spark.sql("SELECT user_exam_id, exam_name, sequences.counts.all_correct, sequences.counts.correct, sequences.counts.incomplete, sequences.counts.incorrect, sequences.counts.submitted, sequences.counts.total, sequences.counts.unanswered FROM exams")
```
```
some_question_info = spark.sql("SELECT user_exam_id, exam_name, sequences.questions[0].user_result as question_0, sequences.questions[1].user_result as question_1, sequences.questions[2].user_result as question_2, sequences.questions[3].user_result as question_3, sequences.questions[4].user_result as question_4, sequences.questions[5].user_result as question_5  FROM exams")
```

Now, I am writting those new dataFrames to hadoop hdfs `using write.parquet()` function.

```
some_exam_info.write.parquet("/tmp/some_exam_info")
some_question_info.write.parquet("/tmp/some_question_info")
```
Now I switch back to another docker-compose terminal and run the following code to check and see if the new dataframe is added to hadoop hdfs:

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

The output is shown below. Looks like the new dataframes `some_exam_info` and `some_question_info` we just extacted are successfully stored in hdfs.
```
Found 6 items
drwxr-xr-x   - root   supergroup          0 2021-03-08 02:14 /tmp/exams_str
drwxr-xr-x   - root   supergroup          0 2021-03-08 02:31 /tmp/extracted_exams_string
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-03-08 01:50 /tmp/hive
drwxr-xr-x   - root   supergroup          0 2021-03-08 04:33 /tmp/some_exam_info
drwxr-xr-x   - root   supergroup          0 2021-03-08 06:27 /tmp/some_question_info
```

As stated before to retrive these tables from Hadoop HDFS, a data scientist can run the following template `my_file = sqlContext.read.parquet('/tmp/myfile')`. E. g. to retreive the table `some_exam_info` that I just created I run: 

```
my_exam_table = sqlContext.read.parquet('/tmp/some_exam_info')
my_exam_table.show()
```
As shown below the exact table is retrived:
```
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
|        user_exam_id|           exam_name|all_correct|correct|incomplete|incorrect|submitted|total|unanswered|
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
|6d4089e4-bde5-4a2...|Normal Forms and ...|      false|      2|         1|        1|        4|    4|         0|
|2fec1534-b41f-441...|Normal Forms and ...|      false|      1|         2|        1|        4|    4|         0|
|8edbc8a8-4d26-429...|The Principles of...|      false|      3|         0|        1|        4|    4|         0|
|c0ee680e-8892-4e6...|The Principles of...|      false|      2|         2|        0|        4|    4|         0|
|e4525b79-7904-405...|Introduction to B...|      false|      3|         0|        1|        4|    4|         0|
|3186dafa-7acf-47e...|        Learning Git|       true|      5|         0|        0|        5|    5|         0|
|48d88326-36a3-4cb...|Git Fundamentals ...|       true|      1|         0|        0|        1|    1|         0|
|bb152d6b-cada-41e...|Introduction to P...|       true|      5|         0|        0|        5|    5|         0|
|70073d6f-ced5-4d0...|Intermediate Pyth...|       true|      4|         0|        0|        4|    4|         0|
|9eb6d4d6-fd1f-4f3...|Introduction to P...|      false|      0|         1|        0|        1|    5|         4|
|093f1337-7090-457...|A Practical Intro...|      false|      3|         1|        0|        4|    4|         0|
|0f576abb-958a-4c0...|Git Fundamentals ...|       true|      1|         0|        0|        1|    1|         0|
|0c18f48c-0018-450...|Introduction to M...|      false|      4|         1|        1|        6|    6|         0|
|b38ac9d8-eef9-495...|   Python Epiphanies|      false|      4|         0|        2|        6|    6|         0|
|bbc9865f-88ef-42e...|Introduction to P...|      false|      4|         1|        0|        5|    5|         0|
|8a0266df-02d7-44e...|Python Data Struc...|      false|      3|         0|        1|        4|    4|         0|
|95d4edb1-533f-445...|Python Data Struc...|      false|      3|         0|        0|        3|    4|         1|
|f9bc1eff-7e54-42a...|Working with Algo...|       true|      4|         0|        0|        4|    4|         0|
|dc4b35a7-399a-4bd...|Learning iPython ...|      false|      2|         0|        0|        2|    4|         2|
|d0f8249a-597e-4e1...|   Python Epiphanies|       true|      6|         0|        0|        6|    6|         0|
+--------------------+--------------------+-----------+-------+----------+---------+---------+-----+----------+
only showing top 20 rows
```

To close up, I used the `exit()` command to exit my spark container, and ^C to exit my kafka logs. I closed connections with both of those windows and I spun down my docker cluster, then checked to make sure there were no docker containers running.

```
docker-compose down
docker-compose ps
docker ps -a
```

#### Summary

In this project, I demonstrated how to publish and consume messages with kafka, consume and print data with spark and write in hadoop hdfs. I have used kafka, zookeeper, spark, hadoop, and the mids containers. I have also used assessment-attempt dataset which is a multi-level nested JSON. In addition, I demonstrated in spark how to transform data, work with nested json structures, and land messages into parquet files into the hdfs directory. As shown, the nested json data is flattened, stored, retrivable and queryable.


