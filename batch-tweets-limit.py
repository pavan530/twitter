import tweepy
from elasticsearch import Elasticsearch
from pymongo import MongoClient
import time
import secrets

es = Elasticsearch([secrets.es_host_name])
auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
auth.set_access_token(secrets.access_token, secrets.access_token_secret)
api = tweepy.API(auth)
#     tweets = api.search("#analytics",count = 100,lang = "en",since = "2016-09-07")
c = tweepy.Cursor(api.search,q="#analytics",since="2015-01-01",lang="en",include_entities=True).items()

def get_db():
    client = MongoClient('localhost:27017')
    db = client.twitter
    return db
    
    
def batch_tweets(db):
    while True:
        try:
            tweet = c.next()
            enhanced = {"text":tweet.text,"location":tweet.user.location,"created_at":tweet.created_at,"user":tweet.user.name,"id":tweet.id}
            print (enhanced)
            es.index(index="batch-index", doc_type='tweet', id=None, body=enhanced)
            db.tweets.insert(enhanced)
            print ("inserted to mongoDB")

        # Insert into db
        except tweepy.TweepError:
            print ("Waiting for time...")
            time.sleep(60 * 15)
            continue
        except StopIteration:
            break
       
if __name__=='__main__':
    db = get_db()
    batch_tweets(db)
    