'''
Created on Feb 27, 2016

@author: shreepad
'''
from slistener import SListener
import time, tweepy, sys

## authentication
consumer_key ="jyhjfONNuDfbHl7nsLAgvGw6f"

consumer_secret = "QOoFzQA1555vIhyiUss20c5eq7uPlV3IEAzbXYRv2dWSJ3rC80"

access_token = "215905178-5WVfGwNA1SKJjCB1WgLcVfEC2nC7pcVkFcGiiRC4"

access_token_secret = "3xoQjM4J2koAZw6fMl60qIZFWcLrOoDNIC9cJdJmhLexQ"

# OAuth process, using the keys and tokens

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



def main():
    
    track = ['dundundundun']
    
    full = [-180,-90,180,90]
    rutgers=[ -74.496245,-40.464329, -74.374364,40.540052 ]
    nj = [-76,38.5,-73.5,41.5]
 
    listen = SListener(api, 'data')
    stream = tweepy.Stream(auth, listen)
    
    #import requests.packages.urllib3
    #requests.packages.urllib3.disable_warnings('SNIMissingWarning')
    #requests.packages.urllib3.disable_warnings('InsecurePlatformWarning')
   

    try: 
    stream.filter(locations = nj)
    print ("Streaming started...")
    except:
        print ("error!")
        stream.disconnect()

if __name__ == '__main__':
    main()
