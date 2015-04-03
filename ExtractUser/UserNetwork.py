#!/usr/bin/python
from __future__ import absolute_import, print_function
import sys
import tweepy
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
#from tweepy import Stream
from tweepy import Friendship, MemoryCache, FileCache, API
from tweepy.parsers import Parser
import time
import ConfigParser
config = ConfigParser.ConfigParser()
config.readfp(open(r'config.txt'))
consumer_key = config.get('twitter', 'consumer_key')
consumer_secret = config.get('twitter', 'consumer_secret')
access_token= config.get('twitter', 'access_token')
access_token_secret= config.get('twitter', 'access_token_secret')
user_list = [] #dictionary for storing user_ids

user_graph_file = "graph_output.txt"
log_file = "log.txt"

def build_user_list(user_file):
	global user_list
	with open(user_file) as users:
		for line in users:
			array = line.split("\t")
			if len(array)==2:
				user_list.append(array[0])
	#print user_dict.keys()
def iterate_user_list():
	global user_list
	count = 0	
	f = open(log_file,'a')
	for user in user_list:
		get_follower(user)
		count = count + 1
		f.write('getting followers of user ith : '+str(count)+ '\n')
	f.close() 

def get_follower(UserId):
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = API(auth)
	f = open(user_graph_file,'a')
	f.write(str(UserId))
	ids = []
	try:
		for page in tweepy.Cursor(api.followers_ids, user_id=UserId).pages():
	   		ids.extend(page)
			time.sleep(60)
	except:
		pass
	for item in ids:	
		f.write(' ' + str(item))
	f.write('\n')
	f.close() 
			
def main(argv):
	if len(argv)==1:
		build_user_list(argv[0])
		iterate_user_list()

if __name__ == "__main__":
   main(sys.argv[1:])

		
