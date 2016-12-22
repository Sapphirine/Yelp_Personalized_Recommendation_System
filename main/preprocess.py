from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
import gensim
import pickle
from gensim import corpora, models

def openfile():
	with open('/Users/xx/Desktop/big_data/project/Yelp-Personalized-Recommendation/flask/business_num_name','rb') as fp:
	    bn = pickle.load(fp,encoding='latin1')
	with open('/Users/xx/Desktop/big_data/project/Yelp-Personalized-Recommendation/flask/business_num_review','rb') as fp:
	    br = pickle.load(fp,encoding='latin1')
	return [bn,br]



def tokenize(br,lda_list,lines_num = -1):
	'''
	:type lines_num: int
	# default set to read all lines in files

	'''	
	'''    
	dataset = []
	for key in br:
		dataset.append(br[key])
	'''
	#lda_list = [1,2,3,4,5,6,7,8,9,10,1,1,1,1,1,1,1]
	tokenizer = RegexpTokenizer(r'[a-zA-Z0-9^\']+')
	i = 0
	testList=[]
	rawList=[]
	bidList=[]
	for line in lda_list:
		raw = str(br[int(line)]).lower()
		tokens = tokenizer.tokenize(raw)
		rawList.append(tokens)
		i+=1
		if i%1000==0:
			print(i)
		if i==lines_num:
			break
	print('tokenization finished')
	return rawList

def stop_words(rawList, lines_num = -1):
	en_stop = get_stop_words('en')
	i=0
	stopped_result=[]
	for item in rawList:
		stopped_tokens = [i for i in item if not i in en_stop]
		stopped_result.append(stopped_tokens)
		i+=1
		if i%1000==0:
			print(i)
		if i==lines_num:
			break
	print('stop words finished')
	return stopped_result

def stem(stopped_result,lines_num=-1):
	p_stemmer = PorterStemmer()
	stem_result=[]
	i=0
	for item in stopped_result:
		texts = [p_stemmer.stem(i) for i in item]
		stem_result.append(texts)
		i+=1
		if i%1000==0:
			print(i)
		if i==lines_num:
			break
	print('stemming finished')
	return stem_result

def doc_term_matrix(processed_data):
	corpusList=[]
	dictionary = corpora.Dictionary(processed_data)
	corpus = [dictionary.doc2bow(text) for text in processed_data]
	return [corpus, dictionary]
	