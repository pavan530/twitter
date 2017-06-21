#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch
from pymongo import MongoClient
import json
import secrets

#Variables that contains the user credentials to access Twitter API 


es = Elasticsearch([secrets.es_host_name])
client = MongoClient('localhost:27017')
db = client.twitter


class StdOutListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        res = es.index(index="streaming-tweets", doc_type='tweets', id=None, body=tweet)
        db.tweet_streaming.insert(tweet)
        print(res['created'])
        print ("Inserted into ES and MongoDB")
        return True

    def on_error(self, status):
        print (status)



if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['#analytics'])