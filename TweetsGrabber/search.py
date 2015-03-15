#!/usr/bin/python
from __future__ import absolute_import, print_function
import sys
import tweepy
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
#from tweepy import Stream
from tweepy import Friendship, MemoryCache, FileCache, API
from tweepy.parsers import Parser

import ConfigParser
config = ConfigParser.ConfigParser()
config.readfp(open(r'config.txt'))
consumer_key = config.get('twitter', 'consumer_key')
consumer_secret = config.get('twitter', 'consumer_secret')
access_token= config.get('twitter', 'access_token')
access_token_secret= config.get('twitter', 'access_token_secret')

def main(argv):
	if len(argv)!=2:
		print (argv)
		return 0
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = API(auth)
	print ('crawl for %s  to  %s ' %(argv[0], argv[1])) 
	f = open(argv[1],'a')
	for tweet in tweepy.Cursor(api.search,
				#	q="$FB",
					q=argv[0],
					rpp=100,
					count=1000,
					result_type="recent",
					include_entities=True,
					lang="en").items():
		#print (tweet.created_at, tweet.text, tweet.retweet_count, tweet.author.location, tweet.author.followers_count,)
		temp_text = tweet.text.replace('\n',' ')
		line = '%s\t%s\t%s\t%s\t%s\t%s\t%s'%(tweet.id,tweet.created_at, temp_text, tweet.retweet_count, tweet.author.location,tweet.author.id,tweet.author.followers_count)
		line = line.replace('\n',' ')
		line = line + '\n'
		f.write(line.encode('utf8'))
	
	f.close() 

#	results = api.search(q="#google")
   #stream = Stream(auth, l)
    #stream.filter(track=['#Valentine'])
if __name__ == "__main__":
   main(sys.argv[1:])
