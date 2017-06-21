import tweepy
from elasticsearch import Elasticsearch


def batch_tweets():
    consumer_key = 'RmDHlUkiz7oCXNja70EOJpQNC'
    consumer_secret = 'HwRGErT2ONHDYqzovA0DyhGESHZW9FKlbQ4zVlDJ2BetxL055h'
    access_token = '2415807788-A13fbTX8mTRMaeo27nxxg84CZl7FH7uiqOCSdy2'
    access_token_secret = 'uT99vkDgavFHz7MXl0CUmEx1z5tjCYPfQRnnxFcoWGRQ8'
#     es = Elasticsearch(["localhost:9200"])
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)    
#     tweets = api.search("#analytics",count = 100,lang = "en",since = "2016-09-07")
    tweets = api.get_status('222525955602788353')
    print (tweets.text)
    #     for tweet in tweepy.Cursor(api.search,q="#analytics",count=100,since="2014-01-01",lang="en").items():
    for tweet in tweets :
        enhanced = {"text":tweet.text,"location":tweet.user.location,"created_at":tweet.created_at,"user":tweet.user.name,"id":tweet.id}
        print (enhanced)
#         es.index(index="batch-index", doc_type='tweet', id=None, body=enhanced)
        
if __name__=='__main__':
    batch_tweets()
