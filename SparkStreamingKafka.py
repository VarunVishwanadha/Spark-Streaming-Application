from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


# Create Spark Context to connect to Spark Cluster
sc = SparkContext(appName="StreamingTweetCount")

sc.setLogLevel("FATAL")

# Set the Batch Interval to 10 sec of Streaming Context
ssc = StreamingContext(sc, 10)

# Setting a checkpoint to allow RDD recovery
ssc.checkpoint("CheckpointTwitterApp")

# Create Kafka Stream to Consume Data from Twitter Topic
# localhost:2181 - Default Zookeeper Consumer Address
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter': 1})


def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# Parse Twitter Data as json
parsed = kafkaStream.window(120, 20).map(lambda v: json.loads(v[1]))

# Count the number of hashtags in the current stream
hashtag_counts = parsed.flatMap(lambda tweet: ((hashtag['text'], 1) for hashtag in tweet['entities']['hashtags']))\
                .reduceByKey(lambda x, y: x + y).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Adding the count of each hashtag to its previous count
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_count)\
                .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Print the hashtags count
hashtag_counts.pprint(15)
hashtag_totals.pprint(15)

# Start Execution of Stream
ssc.start()
ssc.awaitTermination()

