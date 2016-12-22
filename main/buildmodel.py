from preprocess import tokenize,stop_words,stem,doc_term_matrix
from topic_modeling import lda,lsi
from utils import save_file
import pickle





def build_model(lines_num = -1):
	#dataset = open('rawdata.csv', 'r')
	


	rawList=tokenize(lines_num)

	print(rawList[0])
	stopped_result = stop_words(rawList,lines_num)

	#stem_result = stem(stopped_result,lines_num)

	[corpus,dictionary] = doc_term_matrix(stopped_result)
	# it seems like without stem the words makes more sense, still working on it
	
	################
	####  LDA  #####
	################
	#ldaList = lda(corpus, dictionary,lines_num)
	lsiList = lsi(corpus, dictionary,lines_num)


	save_file(lsiList)
	

if __name__ == '__main__':
	build_model(100)
