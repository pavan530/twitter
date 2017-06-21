from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch
import json
import secrets
from kafka import KafkaProducer
import time

# es = Elasticsearch([secrets.es_host_name])

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        print (tweet)
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer = KafkaProducer(value_serializer=lambda data: json.dumps(data))
        print ("inside for loop")
        producer.send('test', {"tweet":data})
        producer.flush()
       
#         res = es.index(index="streaming-tweets", doc_type='tweets', id=None, body=tweet)
#         print(res['created'])
        return True

    def on_error(self, status):
        print (status)



if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.access_token_secret)
    stream = Stream(auth, l)
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['#analytics'])

 
