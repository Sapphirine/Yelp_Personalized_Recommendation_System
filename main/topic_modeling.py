import gensim
from gensim import corpora, models

def lda(corpus, dictionary, lines_num=-1):
	i = 0
	ldaList=[]
	for item in corpus:
		ldamodel = gensim.models.ldamodel.LdaModel([corpus[i]], num_topics=3, id2word = dictionary, passes=20)
		print(i)
		ldaList.append(ldamodel.show_topics(num_topics=2,num_words=6,formatted=False))
		print(ldaList[i])
		i = i+1
		if i==lines_num:
			break
	print("LDA finished")
	return ldaList

def lsi(corpus,dictionary,lines_num=-1):
	i = 0
	lsiList=[]
	for item in corpus:
		lsimodel = gensim.models.lsimodel.LsiModel([corpus[i]], num_topics=3, id2word = dictionary)
		print(i)
		lsiList.append(lsimodel.show_topic(0,10))
		print(lsiList[i])
		i = i+1
		if i==lines_num:
			break
	print("LSI finished")
	return lsiList