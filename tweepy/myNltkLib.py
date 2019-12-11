import nltk
from googletrans import Translator
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import Counter 
from nltk.tokenize import RegexpTokenizer



class words_processing():
	def __init__(self,taglist):
		self.lematizer = WordNetLemmatizer();
		self.stopwords = set(stopwords.words('english'));
		self.taglist = [ self.lematizer.lemmatize(word,pos = "n") for word in taglist];
		self.taglist = list(set(self.taglist));
		self.translator = Translator(timeout = 1);
		self.tokenizer = RegexpTokenizer(r'\w+');

	def convert_to_list(self,dictionary):
		dictlist = [];
		for value in dictionary:
			temp = [value.decode("ascii"),dictionary[value]];
			dictlist.append(temp);
		return dictlist;	

	def convert_to_str(self,data):
		atricle_str = ""
		for element in data:
			atricle_str = atricle_str + str(element) + " ";	
		return atricle_str.lower();

	def translate_article(self,data):
		translated_text = ""
		for element in data:
			try:
				translated_text = translated_text + self.translator.translate(element,dest = 'en').text + " ";
			except:
				print("translation failed")
				pass;
		if(len(translated_text) == 0):
			return -1;
		return translated_text.lower();

	def proccess_text(self,text):
		new_text = []
		text = self.tokenizer.tokenize(text);
		for word in text:
			try:
				word = self.lematizer.lemmatize(word,pos = "n")
				if word not in self.stopwords:
					new_text.append(word.encode("ascii"));
			except:
				pass;
		return new_text;		

	def find_labels(self,data,is_english):
		if(is_english == False):
			text = self.translate_article(data);
		else:
			text = self.convert_to_str(data);
		new_text = self.proccess_text(text);
		results = {}
		for word in self.taglist:
			results[word] = 0;

		for word in new_text:
			if word in self.taglist:
				temp_val = results[word];
				temp_val+=1;
				results[word] = temp_val;
				####
		return results;		
	
	def find_most_common_words(self,data,is_english,Number_of_Words):
		if(is_english == False):
			text = self.translate_article(data);
		else:
			text = self.convert_to_str(data);
		
		text = self.proccess_text(text);
		return Counter(text).most_common(Number_of_Words);
	
	def find_doc_freq(self,text):
		return Counter(text);


