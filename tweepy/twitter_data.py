import json
import simplejson
import sys
import myNltkLib
import threading
from googletrans import Translator
import database
import myNltkLib



class tweet_database:
	def __init__(self):
		#declaring a buff for duplicate tweets detection
		self.buff = [];
		#creating a database_interaction object to write to database
		self.database_obj = database.database_interaction(None,None);
		#creating a database nltk object to process the words of the tweet
		self.text_normalizator = myNltkLib.words_processing([]);

	def process_tweet(self,data):
		tweet = simplejson.loads(data);
		tweet_dict = {};
		#created -> date the tweet created
		tweet_dict['created'] = tweet['created_at'];
		#id of tweet
		tweet_dict['tweet_id'] = tweet['id_str'];
		#check if you can extract the full text
		try:
			tweet_dict['text'] = tweet['extended_tweet']['full_text'];
		except:
			tweet_dict['text'] = tweet['text'];
		tweet_dict['text'] = tweet_dict['text'].replace("'"," ");
		#tweet lang
		tweet_dict['lang'] = tweet['lang']
		#tweet user_id
		tweet_dict['user_id'] = tweet['user']['id']
		#tweet description
		tweet_dict['description'] = tweet['user']['description']
		#followers of the user
		tweet_dict['followers'] = tweet['user']['followers_count']
		#friends that user has
		tweet_dict['friends'] = tweet['user']['friends_count']
		#url of a user 
		tweet_dict['user'] = tweet['user']['url']
		tweet_dict['quote_count'] = tweet['quote_count']
		tweet_dict['reply_count'] = tweet['reply_count']
		tweet_dict['retweet'] = tweet['retweet_count']
		tweet_dict['retweeted'] = tweet['retweeted']
		
		tweet = tweet_dict;
		#check if tweet is dublicated
		if(tweet['text'] not in self.buff):
			self.buff.append(tweet['text']);
			return tweet;
		else: 
			return None;
	
	def send_to_database(self,tweet):
		
		tweet_user = [];
		tweet_to_send = [];
		#first process_tweet
		tweet = self.process_tweet(tweet);
		#check if the tweet has valid information (a text)
		if(tweet != None and len(tweet['text'])>0):
			#send to datase to add the user if he doesn't already exists
			tweet_user.append(str(tweet['user_id']));
			tweet_user.append(tweet['followers']);
			tweet_user.append(tweet['friends']);
			tweet_user.append(tweet['user']);
			self.database_obj.update_tweeter_user(tweet_user);	

			tweet_to_send.append(str(tweet['tweet_id']));
			tweet_to_send.append(str(tweet['user_id']));
			tweet_to_send.append(tweet['created']);
			tweet_to_send.append(tweet['text']);
			#if tweet doesn't exists update tweet_table and word table
			if(self.database_obj.update_tweets_table(tweet_to_send)==0):
				print("Tweet uploaded")
				tweet['text'] = self.text_normalizator.proccess_text(tweet['text'].lower());
				tweet['text'] = self.text_normalizator.convert_to_list(self.text_normalizator.find_doc_freq(tweet['text']));
				self.database_obj.update_words_table_from_tweets(tweet['text'],tweet_to_send[0])




