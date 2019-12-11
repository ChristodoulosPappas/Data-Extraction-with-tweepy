# -*- coding: utf-8 -*-
import tweepy
import json
import simplejson
import twitter_data


class myListener(tweepy.StreamListener):
	def on_data(self,tweet):
		#send tweet
		database.send_to_database(tweet);
			

	def on_error(self,status_code):
		print(status_code)
		if(status_code == 420):
			return False;



tweeter_keys = {'consumer_key': 'h01XHwKIia1WPjKk8xajp5Jb1',
'consumer_secret':'onPVVQWpj2NDK4GnCijbFQapZGQuunkKYADn4Dbcb4JnWTo1u4'
,'token':'1189185667206533122-kpTopAJwCqcFApQN1BkuuGdjmiRZ7t',
'token_secret':'Uz1HgLanTon0pFuJlhGVLhQRN0LSZl2Rnt2p8s9dzxwQ9'};

auth = tweepy.OAuthHandler(tweeter_keys['consumer_key'],tweeter_keys['consumer_secret']);
auth.set_access_token(tweeter_keys['token'],tweeter_keys['token_secret'])
api = tweepy.API(auth)
mylistener = myListener();
database = twitter_data.tweet_database();

number_of_labels = input("Enter number of labels: ");
number_of_labels = int(number_of_labels);
#Get labels
'''
labels = [0 for i in range(number_of_labels)];
for i in range(number_of_labels):
	labels[i] = input("Enter label: ");
'''
#Stream based on labels
stream = tweepy.Stream(auth = api.auth,listener = mylistener,timeout=None);
stream.filter(track = ['bankruptcy']);
