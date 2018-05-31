# Spark-Streaming-Application
Getting hashtag counts from live Twitter Stream using Spark Streaming and Apache Kafka.



Steps to run the application:-

1. Start Apache Kafka

~/Softwares/kafka_2.11-1.1.0/bin/kafka-server-start.sh ~/Softwares/kafka_2.11-1.1.0/config/server.properties

2. Start Apache Hadoop

~/Softwares/hadoop-3.0.0/sbin/start-all.sh

3. Replace the consumer keys and access tokens in TweepyNetworkKafka.py and update the search keywords.

4. Run TweepyNetworkKafka.py

python ~/TweepyNetworkKafka.py

5. Run SparkStreamingKafka.py

python ~/SparkStreamingKafka.py
