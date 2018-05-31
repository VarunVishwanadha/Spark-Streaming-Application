from __future__ import print_function
import tweepy
import json
import pykafka


# Twitter Stream Listener
class KafkaListener(tweepy.streaming.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()
  
    def on_data(self, data):
        # Ignore retweets
        if not ('retweeted_status' in data):
            tweet_json = json.loads(data)
            hashtags = tweet_json['entities']['hashtags']
            if len(hashtags) > 0:
                print("  -  ".join([str(hashtag['text']) for hashtag in hashtags]))
                print(tweet_json['text'])
                self.producer.produce(bytes(data, "ascii"))
        return True
                                                                                                                                           
    def on_error(self, status):
        print(status)
        return True


twitter_keys = json.load(open('twitter_keys.json'))
# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you
consumer_key = twitter_keys["consumer_key"]
consumer_secret = twitter_keys["consumer_secret"]

# Create an access token under the the "Your access token" section
access_token = twitter_keys["access_token"]
access_token_secret = twitter_keys["access_token_secret"]

# Twitter API Auth
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Twitter Stream Config
twitter_stream = tweepy.Stream(auth, KafkaListener())

# Enter the search keywords
keywords = ['NBA', 'NFL', 'NHL', 'MLB']

# Get Tweets that have the provided search words
twitter_stream.filter(track=keywords, languages=['en'])
