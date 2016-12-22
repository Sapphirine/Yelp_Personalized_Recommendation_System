from flask import *
import json
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from buildmodel import build_model
from preprocess import tokenize,stop_words,stem,doc_term_matrix,openfile
from topic_modeling import lda,lsi
#from convertpicklelingyu import init_list_of_objects
import numpy as np
import gensim

app = Flask(__name__)


@app.route('/', methods=['GET','POST'])
def hello():
	if request.method == 'POST':
		conf = SparkConf().setAppName("yelp_recommendation-server")
		sc = SparkContext(conf=conf)
		global sameModel
		sameModel = MatrixFactorizationModel.load(sc, "/Users/xx/Desktop/big_data/project/Yelp-Personalized-Recommendation/flask/dataset/myCollaborativeFilter")
		return redirect(url_for('send'))
	return render_template('hello.html')



@app.route('/send',methods=['GET','POST'])
def send():
	userID = None
	if request.method == 'POST':
		userID = request.form['userID']
		keyword = request.form['keyword']
		print(type(userID))
		print(type(int(userID)))
		result = 1
		result = sameModel.recommendProducts(int(userID),100)
		tmp = {}    # key = business_num, value = business_rating
		lda_list = []
		for line in result:
			tmp[line[1]] = line[2]
			lda_list.append(line[1])
		[bn,br] = openfile()
		name_list = []
		for key in lda_list:
			name_list.append(bn[key])
		Recommendation = name_list[:10]
		#LDA
		if len(request.form['keyword']) >= 1:
			rawList=tokenize(br,lda_list)

			stopped_result = stop_words(rawList)
			[corpus,dictionary] = doc_term_matrix(stopped_result)
			lsiList = lsi(corpus, dictionary)

			
			#word matching
			document = list()
			for i in range(0,99):
				document.append(list())
			for i in range(0,99):
				item = lsiList[i]
				for word in item:
					document[i].append(','.join([str(word[0])]))
			model = gensim.models.Word2Vec(document, min_count=1)
			
			a = [0]*100
			dic = {}
			for i in range(0,99):
				business = document[i]
				for item in business:
					a[i] = a[i] + model.similarity(keyword,item)
				dic[a[i]] = lda_list[i]
					#!!!!!!!!!!!gaichicken!!!!!!!!!#############################
			#a = np.sort(a,axis=0)
			a = sorted(a, reverse=True)
			Recommendation = []
			#a = np.ndarray.tolist(a)
			for key2 in a[0:10]:
				print(key2)
				Recommendation.append(bn[dic[key2]])

		
		#Recommendation = json.dumps(result)
		return render_template('index.html',Recommendation=Recommendation)
	return render_template('index.html')


if __name__ == "__main__":
	app.run(debug=True)