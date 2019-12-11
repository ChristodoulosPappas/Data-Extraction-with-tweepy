#Author: Pappas Christodoulos

import psycopg2
import random

################ QUERIES FOR DATA INSERTION IN TABLES #####################
#	DOC TABLE : 
#	PRIMARY KEY: DOC_ID,  
#	FOREIGN KEY: NEWS_ID,
# 	ATTRIBUTES: URL <= url of the doc, DOC_LEN <= number of words, DOC_DATE <= date that doc released, LANG <= language
insert_doc_table = "INSERT INTO DOC VALUES (";
#	WORDS TABLE:
#	PRIMARY KEY: WORD
#	ATTRIBUTES: DOC_FREQ <= number of documents found containing this word , TWEET_FREQ <= number of tweets that the word found
insert_words_table = "INSERT INTO WORDS VALUES ('";
#	EXIST_IN TABLE
#	PRIMARY KEY: (WORD,NEWS_ID)
#	ATTRIBUTES: COUNT_F <= number of times the word found in the document 
insert_exists_in = "INSERT INTO EXIST_IN VALUES ";
#	EXIST_IN_TWEET 
#	Same as exist_in but for tweets
insert_exists_in_tweet = "INSERT INTO EXIST_IN_TWEET VALUES ";
#	NEWS_SITE TABLE:
#	PRIMARY KEY: NEWS_ID
#	ATTRIBUTES: URL <=  url of news site, lang <= main language of the site 
insert_news_table = "INSERT INTO NEWS_SITE VALUES (";
#	TITLES_IN TABLE:
#	PRIMARY KEY: (WORD,NEWS_ID)
insert_titles_in = "INSERT INTO TITLES_IN VALUES ";
#	TWEETS TABLE:
#	PRIMARY KEY: TWEET_ID
#	FOREIGN_KEY: USER_ID 
#	ATTRIBUTES: DATE <= DATE TWEET PUBLISHED
insert_tweet_table = "INSERT INTO TWEETS VALUES ('";
#	TWEETER_USER TABLE
#	PRIMARY KEY: USER_ID
#	ATTRIBUTES: FOLLOWERS <= number of followers of the user, FRIENDS <= number of friends of the user, URL <url of user
insert_tweeter_user_table = "INSERT INTO TWEETER_USER VALUES ('";
################ QUERIES FOR DATABASE INTERACTION #########################

get_last_words_id = "SELECT MAX(WORDS.WORD_ID) FROM WORDS";
get_last_doc_id = "SELECT MAX(DOC.DOC_ID) FROM DOC";
get_last_news_site_id = "SELECT MAX(NEWS_SITE.NEWS_ID) FROM NEWS_SITE";
check_word_existance = "SELECT WORD FROM WORDS WHERE WORD = '";
search_for_doc = "SELECT DOC_ID FROM DOC WHERE URL = '";
search_for_news_site = "SELECT NEWS_SITE.NEWS_ID FROM NEWS_SITE WHERE NEWS_SITE.URL = '";
get_news_site_data = "SELECT N.NEWS_ID,N.LANG FROM NEWS_SITE N WHERE N.URL = '";
search_for_twitter_user = "SELECT USER_ID FROM TWEETER_USER WHERE USER_ID = '";
search_for_tweet = "SELECT TWEET_ID FROM TWEETS WHERE TWEET_RAW_TEXT = '";


################ QUERIES FOR TABLE UPDATE IF A WORD ALREADY EXISTS ###################
update_word_table_from_doc = "UPDATE WORDS SET DOC_FREQ = DOC_FREQ+1 WHERE WORD = '"
update_word_table_from_tweet = "UPDATE WORDS SET TWEET_FREQ = TWEET_FREQ+1 WHERE WORD = '"


class database_interaction:
	def __init__(self,host,password):
		self.host = host;
		if(host == None):
			self.host = "localhost";	
		self.password = password;	
		if(password == None):
			self.password = "YourPassword"
		
	def connect_to_database(self):
		return psycopg2.connect(dbname = "postgres",host = self.host, user = "postgres", password =self.password );

	def send_data(self):
		con = self.connect_to_database();
		cur = con.cursor();
		counter = 0;
		lines =0 ;
		for line in fd.readlines():
			lines+=1
			try:
				query  = "INSERT INTO URLS VALUES ('"+line[:len(line)-1]+","+self.main_site +"');"
				cur.execute(query);
				con.commit();
			except:
				counter+=1;
				print(line)	
		print(lines)
		print(counter)
		cur.close();
		con.close();
		fd.close();


	def get_site_url(self,news_site_url):	
		
		con = self.connect_to_database();
		cur = con.cursor();
		
		query = "SELECT NEWS_ID FROM NEWS_SITE WHERE URL = '" + news_site_url + "';";
		cur.execute(query); 
		rows = cur.fetchall();
		cur.close();
		con.close();
		return rows[0][0];	

		

	#in this function we insert rows in the news_table	
	def update_news_table(self,news_url,lang):
		con = self.connect_to_database();
		cur = con.cursor();
		#Checking if the site already exists
		query = search_for_news_site + news_url[:len(news_url)-1] + "';";
		cur.execute(query);
		ans = cur.fetchall();	

		##if the site does't exist then we choose a random int
		# if random int already exists we try adding again adding 1;
		if(len(ans) == 0 or ans[0][0] == None):
			new_index = random.randint(0,2147483647); 
			while(True):
				query  = insert_news_table + str(new_index) + ",'" + news_url[:len(news_url)-1] + "','" + lang + "');"
					
				try: 
					cur.execute(query);
					con.commit();
					cur.close();
					con.close();	
					break;
				except:
					new_index +=1;
					cur.close();
					con.close();
					con = self.connect_to_database();
					cur = con.cursor();
		else:
			#if site already exists
			cur.close();
			con.close();
			print("site already exists")

	def get_site_data(self,site_url):
		con = self.connect_to_database();
		cur = con.cursor();
		query  = get_news_site_data + site_url + "';";
		cur.execute(query);
		ans = cur.fetchall();
		if(len(ans) == 0):
			return None,None;
		else:
			return ans[0][0],ans[0][1];

	#In this function we try to insert new documets in the database		
	def update_doc_table(self,doc):
		con = self.connect_to_database();
		cur = con.cursor();
		#doc[0] => doc_url
		#doc[1] => date
		#doc[2] => news_id
		#doc[3] => lan
		#doc[4] =>len
		
		#check if a doc already exists
		query = search_for_doc + doc[0][:len(doc[0])-1] + "';";
		cur.execute(query);
		ans = cur.fetchall();
		#if it does't exist we add the doc in the table
		if(len(ans) == 0 or ans[0][0] == None):
			new_index = random.randint(0,9223372036854775800);
			while(True):
				query = insert_doc_table + str(new_index)+",'"+doc[0][:len(doc[0])-1]+"',"+str(doc[4])+",'" +doc[1][:len(doc[1])-1]+"','"+doc[3]+"',"+str(doc[2])+");";
				try:
					cur.execute(query);
					con.commit();
					cur.close();
					con.close();
					return new_index,False;
				except:
					new_index +=1;
					cur.close();
					con.close();
					con = self.connect_to_database();
					cur = con.cursor();
		#else we return the doc_id and a True flag in order not to continiue updating words table		
		else:
			cur.close();
			con.close();	
			return ans[0][0],True;
		
	
	#In this function we try to insert new tweeter user in the database
	def update_tweeter_user(self,data):
		#data[0] <= user_id
		#data[1] <= folowers
		#data[2] <= friends
		#data[3] <= url
		con = self.connect_to_database();
		cur = con.cursor();
		query = search_for_twitter_user + data[0]+"';";
		cur.execute(query); 
		ans = cur.fetchall();
		if(len(ans) == 0 or ans[0][0] == None):
			if(data[3]!=None):
				query = insert_tweeter_user_table+data[0]+"',"+str(data[1])+","+str(data[2])+",'"+data[3]+"');"; 
			else:
				query = insert_tweeter_user_table+data[0]+"',"+str(data[1])+","+str(data[2])+",null);"; 
				
			cur.execute(query);
			con.commit();
		else:
			print("User already exists");



	#In this function we try to insert a new_tweet		
	def update_tweets_table(self,data):
		#data[0] <= tweet_id
		#data[1] <= user_id
		#data[2] <= date
		#data[3] <= tweet_raw_text

		#we first check if the tweeter already exists
		con = self.connect_to_database();
		cur = con.cursor();
		query = search_for_tweet + data[3] +"';";
		cur.execute(query);
		ans = cur.fetchall();
		#if it doesn't exist we insert the tweet in the table
		if(len(ans) == 0 or ans[0][0] == None):
			query = insert_tweet_table + data[0] + "','" + data[1]+ "','" + data[2] + "','"+data[3] + "');";
			print(query)
			cur.execute(query);
			con.commit();
			return 0;
		else:
			print("Tweet already exists");
			#already exists
			return -1;

	#Update words table	from docs	
	#text => array of arrays with two elemenst: [[word,frequency of word in text],....]
	def update_words_table_from_docs(self,text,doc_id,is_title):
		
		con = self.connect_to_database();
		cur = con.cursor();
		
		for i in range(len(text)):
			#we check if the word already exists	
			query = check_word_existance + text[i][0] + "';"	
			cur.execute(query);
			ans = cur.fetchall();
			#We insert the word if it doesn't exist
			if(len(ans) == 0 or ans[0][0] == None):
				query = insert_words_table + text[i][0] + "'," + str(1) + "," + str(0) + ")"; 
				cur.execute(query);
				con.commit();
			#else we update the doc_freq
			else:
				query = update_word_table_from_doc + text[i][0] + "';"
				cur.execute(query);
				con.commit();
		
		values = ""
		#After we updated the words table then we update the exist_in table
		if(is_title == True):
			for i in range(len(text)):
				query = "(" + str(doc_id) + ",'" + text[i][0] + "')";
				if(i < len(text) - 1):
					values = values + query + ",";
				else:
					values = values + query + ";";	
			query = insert_titles_in + values;
		else:
			for i in range(len(text)):
				query = "(" + str(doc_id) + ",'" + text[i][0] + "'," + str(text[i][1]) + ")";
				if(i < len(text) - 1):
					values = values + query + ",";
				else:
					values = values + query + ";";	
			query = insert_exists_in + values;				
		if(len(text)>1):
			cur.execute(query);
			con.commit();
		con.close();
		cur.close();
	

	#Update words table from tweets	
	#we ousiastika repeat the same procedure as with the function "update_words_table_from_docs"
	def update_words_table_from_tweets(self,text,tweet_id):
		
		con = self.connect_to_database();
		cur = con.cursor();
		print(tweet_id)	
		for i in range(len(text)):
			new_index = random.randint(0,2147483647);
			query = check_word_existance + text[i][0] + "';"	
			cur.execute(query);
			ans = cur.fetchall();
			if(len(ans) == 0 or ans[0][0] == None):
				query = insert_words_table + text[i][0] + "'," + str(0) + "," + str(1) + ")"; 
				cur.execute(query);
				con.commit();
			else:
				query = update_word_table_from_tweet + text[i][0] + "';"
				cur.execute(query);
				con.commit();
		

		values = ""
		for i in range(len(text)):
			query = "('" + tweet_id+ "','" + text[i][0] + "'," + str(text[i][1]) + ")";
			if(i < len(text) - 1):
				values = values + query + ",";
			else:
				values = values + query + ";";	
		if(len(text)>1):
			query = insert_exists_in_tweet + values;				
			cur.execute(query);
			con.commit();
			con.close();
			cur.close();

		
